/**
 * sheetsSyncService.js
 *
 * Sincroniza la colección OlimpiaClients en MongoDB contra un Google Sheet.
 *
 * Estrategia:
 *   - Composite key: RUT + NAME (o COMPANY NAME) — identifica un doc de forma única.
 *   - SHA-256 hash de los campos fuente (sin _id, _syncHash, _syncedAt, ni campos
 *     calculados por la API como deliveryDay/region).
 *   - INSERT  si el doc no existe en Mongo.
 *   - UPDATE  si el hash cambió (algún campo del sheet fue editado).
 *   - SKIP    si el hash es idéntico (sin operación de escritura).
 *   - DELETE  hard delete si el doc existe en Mongo pero ya no está en el sheet.
 *
 * Variables de entorno requeridas:
 *   GOOGLE_SPREADSHEET_ID       ID del spreadsheet (extraído de la URL de Google Sheets)
 *   GOOGLE_SHEETS_RANGE         Rango a leer, ej: "Clientes!A1:Z" (default: "Sheet1")
 *   MONGO_URI                   URI de conexión MongoDB
 *   MONGO_CLIENTS_DB_NAME       Nombre del DB (default: chatbot)
 *   MONGO_CLIENTS_COLLECTION    Nombre de la colección (default: OlimpiaClients)
 *
 * Opcional:
 *   SHEETS_SYNC_INTERVAL_HOURS  Intervalo en horas para sync automático (0 = desactivado)
 *   SHEETS_SYNC_SECRET          Token Bearer para el endpoint manual
 *
 * Credenciales Google:
 *   Archivo: credentials/google/service-account.json
 *   Permisos necesarios en el service account: "Sheets API" → readonly
 *   El sheet debe compartirse con el email del service account.
 *
 * Ejemplo Apps Script para trigger desde el Sheet:
 *
 *   function syncToMongo() {
 *     var url    = "https://tu-api.com/helpers/sync-knowledgebase";
 *     var secret = "tu-secret-aqui";
 *     var res = UrlFetchApp.fetch(url, {
 *       method: "post",
 *       headers: { Authorization: "Bearer " + secret },
 *       muteHttpExceptions: true
 *     });
 *     Logger.log(res.getContentText());
 *   }
 *
 *   // Para agregar un botón: Insertar → Imagen → botón dibujado → asignar función syncToMongo
 */

import { createHash }    from 'crypto';
import { google }        from 'googleapis';
import path              from 'path';
import { fileURLToPath } from 'url';
import fs                from 'fs';
import { MongoClient, ObjectId } from 'mongodb';
import XLSX              from 'xlsx';
import {
    ensureTTLIndexes,
    getSnapshotsCollection
} from './auditStore.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// Ruta al service account key
const KEY_FILE_PATH = path.resolve(process.cwd(), 'credentials/google/service-account.json');

// Campos calculados por la API — excluidos del hash para no generar falsos positivos
const EXCLUDED_FROM_HASH = new Set(['_id', '_syncHash', '_syncedAt', 'deliveryDay', 'region']);

// ── MongoDB ──────────────────────────────────────────────────────────────────
let _client = null;

async function getCollection() {
    if (!_client) {
        const uri = process.env.MONGO_URI;
        if (!uri) throw new Error('MONGO_URI no está definido en .env');
        _client = new MongoClient(uri);
        await _client.connect();
    }
    const dbName  = process.env.MONGO_CLIENTS_DB_NAME  || 'chatbot';
    const colName = process.env.MONGO_CLIENTS_COLLECTION || 'OlimpiaClients';
    return _client.db(dbName).collection(colName);
}

async function getOlimpiaClientsClient() {
    await getCollection(); // garantiza _client conectado
    return _client;
}

// Guardrail por defecto: abortar si las filas nuevas son menos del 50% del estado actual.
const DEFAULT_MIN_ROWS_PERCENT = 0.5;
// Regex estricto para detectar (no para filtrar) RUTs con formato canónico chileno.
const STRICT_RUT_REGEX = /^(?:\d{1,3}\.\d{3}\.\d{3}|\d{7,9})-[\dkK]$/;

/**
 * Limpia una fila para insertarla en OlimpiaClients:
 *   - Aplana line endings (\r\r\n, \r\n, \r → \n)
 *   - Trim de espacios al inicio/fin de strings
 *   - NO toca formato de precios (preserva como vienen)
 *   - Quita campos internos del snapshot (_compositeKey, _syncHash)
 *   - Agrega campos de auditoría (_appliedFromSnapshotId, _appliedAt)
 */
function normalizeRowForMongo(rawRow, snapshotObjectId, appliedAt) {
    const out = {};
    for (const [key, value] of Object.entries(rawRow)) {
        if (key.startsWith('_')) continue; // descarta _compositeKey, _syncHash y _id del snapshot
        if (typeof value === 'string') {
            out[key] = value
                .replace(/\r\r\n/g, '\n')
                .replace(/\r\n/g, '\n')
                .replace(/\r/g, '\n')
                .trim();
        } else {
            out[key] = value;
        }
    }
    out._appliedFromSnapshotId = snapshotObjectId;
    out._appliedAt = appliedAt;
    return out;
}

/**
 * Aplica un snapshot guardado a OlimpiaClients usando hard reset atómico
 * (deleteMany + insertMany en transacción). Sólo se aplica si pasa el guardrail.
 *
 * @param {string|ObjectId} snapshotIdInput
 * @param {object} options
 * @param {boolean} options.force          — bypass del guardrail (default false)
 * @param {number}  options.minRowsPercent — porcentaje mínimo del current count (default 0.5)
 * @returns {Promise<object>} stats del apply
 */
export async function applySnapshotToOlimpiaClients(snapshotIdInput, options = {}) {
    const force = Boolean(options.force);
    const minRowsPercent = Number.isFinite(options.minRowsPercent)
        ? options.minRowsPercent
        : DEFAULT_MIN_ROWS_PERCENT;

    const snapshotId = typeof snapshotIdInput === 'string'
        ? new ObjectId(snapshotIdInput)
        : snapshotIdInput;

    const snapshotsCol = await getSnapshotsCollection();
    const snapshot = await snapshotsCol.findOne({ _id: snapshotId });
    if (!snapshot) {
        throw new Error(`Snapshot ${snapshotIdInput} no encontrado en Olimpia._sheetSyncSnapshots`);
    }
    if (!Array.isArray(snapshot.rows) || snapshot.rows.length === 0) {
        throw new Error('Snapshot vacío — abortando para no destruir OlimpiaClients');
    }

    // Filtra filas con RUT (sin RUT no vale para Mongo: el resto del código matchea por RUT)
    const validRows = snapshot.rows.filter((r) => String(r.RUT || '').trim() !== '');
    if (validRows.length === 0) {
        throw new Error('Snapshot sin filas con RUT — abortando para no destruir OlimpiaClients');
    }

    const olimpiaClientsCol = await getCollection();
    const currentCount = await olimpiaClientsCol.countDocuments();

    // Guardrail
    if (!force && currentCount > 0) {
        const minRequired = Math.floor(currentCount * minRowsPercent);
        if (validRows.length < minRequired) {
            throw new Error(
                `Guardrail (${Math.round(minRowsPercent * 100)}%): snapshot trae ${validRows.length} filas con RUT, ` +
                `menos del mínimo de ${minRequired} (current=${currentCount}). ` +
                `No se aplica para evitar borrado masivo accidental. ` +
                `Si esto es intencional, llamar al endpoint con ?force=true.`
            );
        }
    }

    // Detecta duplicados por composite key (NO los filtra — se insertan ambos)
    const keysSeen = new Map();
    const duplicateSamples = [];
    for (let i = 0; i < validRows.length; i += 1) {
        const r = validRows[i];
        const compositeKey = `${String(r.RUT).trim()}::${String(r.NAME || r['COMPANY NAME'] || '').trim().toLowerCase()}`;
        if (keysSeen.has(compositeKey)) {
            duplicateSamples.push({
                key: compositeKey,
                firstSnapshotIndex: keysSeen.get(compositeKey),
                duplicateSnapshotIndex: i
            });
        } else {
            keysSeen.set(compositeKey, i);
        }
    }
    if (duplicateSamples.length > 0) {
        console.warn(`[applySnapshot] ${duplicateSamples.length} duplicados por composite key (se insertan ambos):`);
        for (const d of duplicateSamples.slice(0, 5)) {
            console.warn(`  ${d.key}`);
        }
    }

    // Detecta RUTs malformados (NO los filtra — se insertan tal cual, sólo log warning)
    const malformedRutSamples = [];
    for (const r of validRows) {
        if (!STRICT_RUT_REGEX.test(String(r.RUT).trim())) {
            malformedRutSamples.push({ rut: r.RUT, name: r.NAME || r['COMPANY NAME'] || '' });
        }
    }
    if (malformedRutSamples.length > 0) {
        console.warn(`[applySnapshot] ${malformedRutSamples.length} RUTs con formato no canónico (se insertan tal cual):`);
        for (const m of malformedRutSamples.slice(0, 5)) {
            console.warn(`  "${m.rut}" → ${m.name}`);
        }
    }

    // Normaliza cada fila para insertar
    const appliedAt = new Date();
    const docsToInsert = validRows.map((r) => normalizeRowForMongo(r, snapshotId, appliedAt));

    // Hard reset atómico: deleteMany + insertMany en una transacción
    const client = await getOlimpiaClientsClient();
    const session = client.startSession();
    let deletedCount = 0;
    let insertedCount = 0;
    try {
        await session.withTransaction(async () => {
            const deleteRes = await olimpiaClientsCol.deleteMany({}, { session });
            const insertRes = await olimpiaClientsCol.insertMany(docsToInsert, { session, ordered: true });
            deletedCount = deleteRes.deletedCount;
            insertedCount = insertRes.insertedCount;
        });
    } finally {
        await session.endSession();
    }

    // Marca el snapshot como aplicado
    await snapshotsCol.updateOne(
        { _id: snapshotId },
        { $set: { appliedAt, appliedRowsCount: insertedCount } }
    );

    return {
        snapshotId: String(snapshotId),
        snapshotTotalRows: snapshot.totalRows,
        validRowsCount: validRows.length,
        deletedFromOlimpiaClients: deletedCount,
        insertedToOlimpiaClients: insertedCount,
        duplicatesDetected: duplicateSamples.length,
        malformedRutsDetected: malformedRutSamples.length,
        guardrailPercent: minRowsPercent,
        forceBypass: force,
        appliedAt: appliedAt.toISOString()
    };
}

// ── Google Sheets ────────────────────────────────────────────────────────────
const GOOGLE_SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'];

/**
 * Resuelve las credenciales del service account en este orden de preferencia:
 *   1. GOOGLE_SERVICE_ACCOUNT_JSON  → contenido del JSON inline (recomendado para Render)
 *   2. GOOGLE_APPLICATION_CREDENTIALS → path absoluto al archivo (estándar Google)
 *   3. KEY_FILE_PATH                  → path local del repo (default para dev)
 *
 * Devuelve un objeto descriptivo con `source`, `credentials` o `keyFile`,
 * y un mensaje de error si nada está disponible.
 */
function resolveServiceAccountSource() {
    const inlineJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
    if (inlineJson && inlineJson.trim()) {
        try {
            const parsed = JSON.parse(inlineJson);
            if (parsed.type !== 'service_account') {
                throw new Error(`Tipo esperado "service_account", encontrado "${parsed.type}"`);
            }
            return { source: 'env:GOOGLE_SERVICE_ACCOUNT_JSON', credentials: parsed };
        } catch (err) {
            throw new Error(
                `GOOGLE_SERVICE_ACCOUNT_JSON está seteado pero no es JSON válido: ${err.message}`
            );
        }
    }

    const envKeyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    if (envKeyFile && fs.existsSync(envKeyFile)) {
        return { source: 'env:GOOGLE_APPLICATION_CREDENTIALS', keyFile: envKeyFile };
    }

    if (fs.existsSync(KEY_FILE_PATH)) {
        return { source: 'file:KEY_FILE_PATH', keyFile: KEY_FILE_PATH };
    }

    throw new Error(
        `No se encontraron credenciales del service account. Opciones:\n` +
        `  1. Setear env GOOGLE_SERVICE_ACCOUNT_JSON con el contenido del JSON\n` +
        `  2. Setear env GOOGLE_APPLICATION_CREDENTIALS con el path al archivo (Render Secret File)\n` +
        `  3. Colocar el archivo en ${KEY_FILE_PATH} (solo dev local)`
    );
}

function getGoogleAuth() {
    const src = resolveServiceAccountSource();
    if (src.credentials) {
        return new google.auth.GoogleAuth({ credentials: src.credentials, scopes: GOOGLE_SCOPES });
    }
    return new google.auth.GoogleAuth({ keyFile: src.keyFile, scopes: GOOGLE_SCOPES });
}

/**
 * Descarga todas las filas del sheet configurado.
 * La primera fila se usa como headers.
 * @returns {Promise<object[]>}
 */
export async function fetchSheetRows() {
    const spreadsheetId = process.env.GOOGLE_SPREADSHEET_ID;
    const range         = process.env.GOOGLE_SHEETS_RANGE || 'Sheet1';

    if (!spreadsheetId) throw new Error('GOOGLE_SPREADSHEET_ID no definido en .env');

    const auth   = getGoogleAuth();
    const sheets = google.sheets({ version: 'v4', auth });

    const response = await sheets.spreadsheets.values.get({ spreadsheetId, range });
    const values   = response.data.values;

    if (!values || values.length < 2) return [];

    const headers = values[0].map(h => h.trim());

    return values.slice(1)
        .map(row => {
            const obj = {};
            headers.forEach((h, i) => {
                obj[h] = (row[i] !== undefined ? row[i] : '').toString().trim();
            });
            return obj;
        })
        .filter(r => (r['RUT'] || '').trim() !== '');   // Filtra filas sin RUT
}

// ── Helpers internos ─────────────────────────────────────────────────────────
/**
 * Clave compuesta estable: RUT + nombre normalizado.
 * Permite identificar correctamente múltiples sucursales del mismo RUT.
 */
function makeCompositeKey(doc) {
    const rut  = (doc['RUT'] || '').trim();
    const name = (doc['NAME'] || doc['COMPANY NAME'] || '').trim().toLowerCase();
    return `${rut}::${name}`;
}

/**
 * SHA-256 de los campos fuente (excluyendo campos calculados por la API).
 * Si ningún campo del sheet cambia, el hash es idéntico → SKIP.
 */
function computeHash(row) {
    const stable = {};
    Object.keys(row).sort().forEach(k => {
        if (EXCLUDED_FROM_HASH.has(k)) return;
        const v = row[k];
        stable[k] = typeof v === 'string' ? v.trim() : v;
    });
    return createHash('sha256').update(JSON.stringify(stable)).digest('hex');
}

// ── Función principal ────────────────────────────────────────────────────────
/**
 * Ejecuta la sincronización completa Sheet → MongoDB.
 *
 * @returns {Promise<{inserted, updated, skipped, deleted, total, durationMs}>}
 */
export async function syncKnowledgebase(options = {}) {
    const t0 = Date.now();

    // 1. Leer Sheet
    const sheetRows = await fetchSheetRows();
    if (sheetRows.length === 0) {
        throw new Error(
            'El sheet no devolvió filas. ' +
            'Verifica GOOGLE_SPREADSHEET_ID, GOOGLE_SHEETS_RANGE, y que el sheet esté compartido con el service account.'
        );
    }
    console.log(`[sheetsSyncService] ${sheetRows.length} filas obtenidas del sheet`);

    // 2. Persistir snapshot en Olimpia._sheetSyncSnapshots (siempre — audit trail)
    const snapshotIdString = await saveSheetSnapshotToAuditDb(sheetRows);
    console.log(`[sheetsSyncService] Snapshot guardado en Olimpia._sheetSyncSnapshots: ${snapshotIdString}`);

    // 3. Aplicar a OlimpiaClients (hard reset atómico con guardrail)
    const applyResult = await applySnapshotToOlimpiaClients(snapshotIdString, options);
    console.log(
        `[sheetsSyncService] OlimpiaClients hard-reset: ` +
        `deleted=${applyResult.deletedFromOlimpiaClients} inserted=${applyResult.insertedToOlimpiaClients} ` +
        `dupes=${applyResult.duplicatesDetected} badRuts=${applyResult.malformedRutsDetected}`
    );

    // 4. Excel local SOLO en development (en producción /tmp es efímero)
    let localExcelPath = null;
    if (process.env.NODE_ENV !== 'production') {
        try {
            localExcelPath = await dumpSheetRowsToExcel(sheetRows);
            console.log(`[sheetsSyncService] (dev) Snapshot Excel local: ${localExcelPath}`);
        } catch (err) {
            console.error('[sheetsSyncService] (dev) No se pudo guardar Excel local:', err.message);
        }
    }

    return {
        mode: 'APPLIED_HARD_RESET',
        snapshotId: snapshotIdString,
        applyResult,
        localExcelPath,
        durationMs: Date.now() - t0
    };
}

/**
 * Inserta un snapshot completo de las filas del Sheet en
 * Olimpia._sheetSyncSnapshots, agregando _compositeKey y _syncHash a cada fila.
 * @param {object[]} sheetRows
 * @returns {Promise<string>} _id (string) del documento insertado
 */
async function saveSheetSnapshotToAuditDb(sheetRows) {
    await ensureTTLIndexes();
    const col = await getSnapshotsCollection();
    const now = new Date();
    const enrichedRows = sheetRows.map((row) => ({
        ...row,
        _compositeKey: makeCompositeKey(row),
        _syncHash: computeHash(row)
    }));
    const doc = {
        createdAt: now,
        spreadsheetId: process.env.GOOGLE_SPREADSHEET_ID || '',
        range: process.env.GOOGLE_SHEETS_RANGE || 'Sheet1',
        totalRows: sheetRows.length,
        rows: enrichedRows
    };
    const result = await col.insertOne(doc);
    return String(result.insertedId);
}

/**
 * Vuelca todas las filas del Sheet a un archivo Excel local.
 * Genera dos hojas: "rows" con datos crudos del sheet + columna `_syncHash`
 * calculada igual que el sync original, y "meta" con el resumen.
 * @param {object[]} sheetRows
 * @returns {Promise<string>} Path absoluto del archivo generado
 */
async function dumpSheetRowsToExcel(sheetRows) {
    const outputDir = path.resolve(process.cwd(), 'tmp', 'sheet-sync-snapshots');
    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
    }

    const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const outputPath = path.join(outputDir, `sheet-snapshot-${stamp}.xlsx`);

    const headers = Object.keys(sheetRows[0] || {});
    const rowsWithHash = sheetRows.map((row) => ({
        ...row,
        _compositeKey: makeCompositeKey(row),
        _syncHash: computeHash(row)
    }));

    const workbook = XLSX.utils.book_new();
    const rowsSheet = XLSX.utils.json_to_sheet(rowsWithHash, {
        header: [...headers, '_compositeKey', '_syncHash']
    });
    XLSX.utils.book_append_sheet(workbook, rowsSheet, 'rows');

    const metaRows = [
        { campo: 'generado_en', valor: new Date().toISOString() },
        { campo: 'total_filas', valor: sheetRows.length },
        { campo: 'spreadsheet_id', valor: process.env.GOOGLE_SPREADSHEET_ID || '' },
        { campo: 'range', valor: process.env.GOOGLE_SHEETS_RANGE || 'Sheet1' },
        { campo: 'mongo_db', valor: process.env.MONGO_CLIENTS_DB_NAME || 'chatbot' },
        { campo: 'mongo_collection', valor: process.env.MONGO_CLIENTS_COLLECTION || 'OlimpiaClients' },
        { campo: 'mongo_writes', valor: 'DISABLED — snapshot only' }
    ];
    const metaSheet = XLSX.utils.json_to_sheet(metaRows);
    XLSX.utils.book_append_sheet(workbook, metaSheet, 'meta');

    XLSX.writeFile(workbook, outputPath);
    return outputPath;
}

// ── Preflight (dry-run, cero escrituras) ─────────────────────────────────────
/**
 * Valida cada capa del pipeline de sincronización sin escribir ni modificar nada.
 * Devuelve un reporte paso a paso y el diff completo que aplicaría syncKnowledgebase().
 *
 * Pasos:
 *   1. envVars            — Variables de entorno requeridas presentes
 *   2. serviceAccountFile — Archivo JSON de service account existe y es JSON válido
 *   3. fetchSheetRows     — Autenticación Google + lectura real del sheet
 *   4. rowStructure       — Calidad de datos: campos requeridos, duplicados de clave compuesta
 *   5. mongoConnect       — Conexión MongoDB (ping)
 *   6. mongoCollectionRead— Lectura de la colección actual (solo proyección mínima)
 *   7. computeDiff        — Diff completo: inserts / updates / skips / deletes (sin escribir)
 *
 * @returns {Promise<object>} Reporte detallado
 */
export async function preflightSyncKnowledgebase() {
    const t0 = Date.now();
    const steps = [];
    let sheetRows = null;
    let mongoDocs = null;
    let preflightMongoClient = null;

    const runStep = async (name, fn) => {
        const start = Date.now();
        try {
            const details = await fn();
            steps.push({ step: name, pass: true, durationMs: Date.now() - start, details: details || {} });
            return { pass: true, data: details };
        } catch (err) {
            steps.push({ step: name, pass: false, durationMs: Date.now() - start, error: err?.message || String(err) });
            return { pass: false, data: null };
        }
    };

    // ── Paso 1: Variables de entorno ─────────────────────────────────────────
    await runStep('envVars', () => {
        const required = {
            GOOGLE_SPREADSHEET_ID: process.env.GOOGLE_SPREADSHEET_ID,
            MONGO_URI: process.env.MONGO_URI,
        };
        const optional = {
            GOOGLE_SHEETS_RANGE: process.env.GOOGLE_SHEETS_RANGE || '(default: Sheet1)',
            MONGO_CLIENTS_DB_NAME: process.env.MONGO_CLIENTS_DB_NAME || '(default: chatbot)',
            MONGO_CLIENTS_COLLECTION: process.env.MONGO_CLIENTS_COLLECTION || '(default: OlimpiaClients)',
            SHEETS_SYNC_SECRET: process.env.SHEETS_SYNC_SECRET ? '(set)' : '(not set — endpoint is public)',
        };
        const missing = Object.entries(required).filter(([, v]) => !v).map(([k]) => k);
        if (missing.length > 0) {
            throw new Error(`Variables de entorno faltantes: ${missing.join(', ')}`);
        }
        return { required: Object.fromEntries(Object.entries(required).map(([k]) => [k, '(set)'])), optional };
    });

    // ── Paso 2: Service account credentials (env var inline o archivo) ───────
    await runStep('serviceAccountFile', () => {
        const src = resolveServiceAccountSource();
        let parsed;
        if (src.credentials) {
            parsed = src.credentials;
        } else {
            const raw = fs.readFileSync(src.keyFile, 'utf8');
            try {
                parsed = JSON.parse(raw);
            } catch {
                throw new Error(`El archivo existe pero no es JSON válido: ${src.keyFile}`);
            }
        }
        const requiredFields = ['type', 'project_id', 'client_email', 'private_key'];
        const missingFields = requiredFields.filter(f => !parsed[f]);
        if (missingFields.length > 0) {
            throw new Error(`Campos faltantes en service account: ${missingFields.join(', ')}`);
        }
        if (parsed.type !== 'service_account') {
            throw new Error(`tipo esperado "service_account", encontrado "${parsed.type}"`);
        }
        return {
            source: src.source,
            path: src.keyFile || '(env inline)',
            project_id: parsed.project_id,
            client_email: parsed.client_email,
            type: parsed.type,
        };
    });

    // ── Paso 3: Fetch sheet rows (autenticación + lectura real) ───────────────
    const fetchStep = await runStep('fetchSheetRows', async () => {
        const rows = await fetchSheetRows();
        sheetRows = rows;
        if (rows.length === 0) {
            throw new Error(
                'El sheet devolvió 0 filas. Verifica GOOGLE_SPREADSHEET_ID, GOOGLE_SHEETS_RANGE ' +
                'y que el sheet esté compartido con el service account.'
            );
        }
        const sampleKeys = Object.keys(rows[0] || {});
        return {
            rowCount: rows.length,
            headers: sampleKeys,
            sampleRow: rows[0],
        };
    });

    // ── Paso 4: Estructura y calidad de filas ─────────────────────────────────
    if (fetchStep.pass && sheetRows) {
        await runStep('rowStructure', () => {
            const issues = [];
            let rowsWithoutRut = 0;
            let rowsWithoutName = 0;
            const compositeKeysSeen = new Map();
            const duplicateKeys = [];

            for (let i = 0; i < sheetRows.length; i++) {
                const row = sheetRows[i];
                const rut = (row['RUT'] || '').trim();
                const name = (row['NAME'] || row['COMPANY NAME'] || '').trim();

                if (!rut) rowsWithoutRut++;
                if (!name) rowsWithoutName++;

                const key = `${rut}::${name.toLowerCase()}`;
                if (key !== '::') {
                    if (compositeKeysSeen.has(key)) {
                        duplicateKeys.push({ key, firstSeenAt: compositeKeysSeen.get(key), duplicateAt: i + 2 });
                    } else {
                        compositeKeysSeen.set(key, i + 2); // +2: 1 for header, 1 for 1-based index
                    }
                }
            }

            if (rowsWithoutRut > 0) issues.push(`${rowsWithoutRut} fila(s) sin campo RUT (serán ignoradas)`);
            if (rowsWithoutName > 0) issues.push(`${rowsWithoutName} fila(s) sin NAME ni COMPANY NAME (clave compuesta incompleta)`);
            if (duplicateKeys.length > 0) {
                issues.push(`${duplicateKeys.length} clave(s) compuesta(s) duplicada(s) — solo la primera ocurrencia se sincronizará`);
            }

            const hasRutColumn = Object.keys(sheetRows[0] || {}).includes('RUT');
            if (!hasRutColumn) {
                throw new Error('El sheet no tiene columna "RUT". Verifica GOOGLE_SHEETS_RANGE y el nombre de la hoja.');
            }

            return {
                totalRows: sheetRows.length,
                rowsWithoutRut,
                rowsWithoutName,
                duplicateKeyCount: duplicateKeys.length,
                duplicateSamples: duplicateKeys.slice(0, 5),
                issues,
                healthy: issues.length === 0,
            };
        });
    } else {
        steps.push({ step: 'rowStructure', pass: false, skipped: true, reason: 'skipped: fetchSheetRows falló' });
    }

    // ── Paso 5: Conexión MongoDB ──────────────────────────────────────────────
    const mongoStep = await runStep('mongoConnect', async () => {
        const uri = process.env.MONGO_URI;
        preflightMongoClient = new MongoClient(uri);
        await preflightMongoClient.connect();
        await preflightMongoClient.db('admin').command({ ping: 1 });
        return { status: 'ok' };
    });

    // ── Paso 6: Lectura de colección ──────────────────────────────────────────
    const mongoReadStep = mongoStep.pass
        ? await runStep('mongoCollectionRead', async () => {
            const dbName = process.env.MONGO_CLIENTS_DB_NAME || 'chatbot';
            const colName = process.env.MONGO_CLIENTS_COLLECTION || 'OlimpiaClients';
            const col = preflightMongoClient.db(dbName).collection(colName);
            mongoDocs = await col.find(
                {},
                { projection: { _id: 1, _syncHash: 1, RUT: 1, NAME: 1, 'COMPANY NAME': 1 } }
            ).toArray();
            return {
                db: dbName,
                collection: colName,
                currentDocCount: mongoDocs.length,
            };
        })
        : (() => {
            steps.push({ step: 'mongoCollectionRead', pass: false, skipped: true, reason: 'skipped: mongoConnect falló' });
            return { pass: false };
        })();

    // ── Paso 7: Diff dry-run (cero escrituras) ────────────────────────────────
    if (fetchStep.pass && mongoReadStep.pass && sheetRows && mongoDocs) {
        await runStep('computeDiff', () => {
            // Construir índice sheet
            const sheetIndex = new Map();
            for (const row of sheetRows) {
                const key = makeCompositeKey(row);
                if (key === '::') continue;
                if (!sheetIndex.has(key)) {
                    sheetIndex.set(key, { ...row, _syncHash: computeHash(row) });
                }
            }

            // Construir índice mongo
            const mongoIndex = new Map();
            for (const doc of mongoDocs) {
                mongoIndex.set(makeCompositeKey(doc), doc);
            }

            const toInsert = [];
            const toUpdate = [];
            const toSkip = [];
            const toDelete = [];

            for (const [key, row] of sheetIndex) {
                const existing = mongoIndex.get(key);
                const label = { key, rut: row['RUT'], name: row['NAME'] || row['COMPANY NAME'] || '' };
                if (!existing) {
                    toInsert.push(label);
                } else if (existing._syncHash !== row._syncHash) {
                    toUpdate.push(label);
                } else {
                    toSkip.push(label);
                }
            }

            for (const [key, doc] of mongoIndex) {
                if (!sheetIndex.has(key)) {
                    toDelete.push({ key, rut: doc['RUT'], name: doc['NAME'] || doc['COMPANY NAME'] || '' });
                }
            }

            return {
                dryRun: true,
                toInsert: toInsert.length,
                toUpdate: toUpdate.length,
                toSkip: toSkip.length,
                toDelete: toDelete.length,
                totalSheetRows: sheetIndex.size,
                totalMongoRows: mongoIndex.size,
                samples: {
                    inserts: toInsert.slice(0, 5),
                    updates: toUpdate.slice(0, 5),
                    deletes: toDelete.slice(0, 5),
                },
            };
        });
    } else {
        steps.push({ step: 'computeDiff', pass: false, skipped: true, reason: 'skipped: pasos anteriores fallaron' });
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────
    if (preflightMongoClient) {
        try { await preflightMongoClient.close(); } catch { /* ignore */ }
    }

    const failedSteps = steps.filter(s => !s.pass);
    return {
        allPassed: failedSteps.length === 0,
        passedCount: steps.filter(s => s.pass).length,
        failedCount: failedSteps.length,
        totalDurationMs: Date.now() - t0,
        firstFailure: failedSteps[0] || null,
        steps,
    };
}
