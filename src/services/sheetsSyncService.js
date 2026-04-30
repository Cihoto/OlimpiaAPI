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
import { MongoClient }   from 'mongodb';

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

// ── Google Sheets ────────────────────────────────────────────────────────────
function getGoogleAuth() {
    if (!fs.existsSync(KEY_FILE_PATH)) {
        throw new Error(
            `Service account key no encontrado en: ${KEY_FILE_PATH}\n` +
            `Coloca el archivo JSON de la cuenta de servicio en esa ruta.`
        );
    }
    return new google.auth.GoogleAuth({
        keyFile: KEY_FILE_PATH,
        scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });
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
export async function syncKnowledgebase() {
    const t0 = Date.now();
    const stats = { inserted: 0, updated: 0, skipped: 0, deleted: 0, total: 0 };

    // 1. Leer sheet
    const sheetRows = await fetchSheetRows();
    stats.total = sheetRows.length;

    // DEBUG: log de la primera fila para verificar formato
    console.log(`[sheetsSyncService] ${sheetRows.length} filas obtenidas del sheet. Ejemplo:`);
    console.log(JSON.stringify(sheetRows[0], null, 2));

    //return boolean para probar endpoint sin hacer nada, luego comentar para activar la lógica completa
    return true;

    if (sheetRows.length === 0) {
        throw new Error(
            'El sheet no devolvió filas. ' +
            'Verifica GOOGLE_SPREADSHEET_ID, GOOGLE_SHEETS_RANGE, y que el sheet esté compartido con el service account.'
        );
    }

    // 2. Construir índice sheet con hash precalculado
    const sheetIndex = new Map();
    for (const row of sheetRows) {
        const key = makeCompositeKey(row);
        if (key === '::') continue;  // Fila sin RUT ni NAME — saltar
        sheetIndex.set(key, { ...row, _syncHash: computeHash(row) });
    }

    // 3. Leer MongoDB (solo campos necesarios para comparar — sin traer todo el doc)
    const col = await getCollection();
    const mongoDocs = await col.find(
        {},
        { projection: { _id: 1, _syncHash: 1, RUT: 1, NAME: 1, 'COMPANY NAME': 1 } }
    ).toArray();

    const mongoIndex = new Map();
    for (const doc of mongoDocs) {
        mongoIndex.set(makeCompositeKey(doc), doc);
    }

    // 4. Upsert: insertar nuevos, actualizar cambiados, saltear idénticos
    for (const [key, row] of sheetIndex) {
        const existing = mongoIndex.get(key);

        if (!existing) {
            await col.insertOne({ ...row, _syncedAt: new Date() });
            stats.inserted++;
        } else if (existing._syncHash !== row._syncHash) {
            await col.updateOne(
                { _id: existing._id },
                { $set: { ...row, _syncedAt: new Date() } }
            );
            stats.updated++;
        } else {
            stats.skipped++;
        }
    }

    // 5. Hard delete: en Mongo pero ya no en el sheet
    for (const [key, doc] of mongoIndex) {
        if (!sheetIndex.has(key)) {
            await col.deleteOne({ _id: doc._id });
            stats.deleted++;
        }
    }

    stats.durationMs = Date.now() - t0;

    console.log(
        `[sheetsSyncService] Sync completado — ` +
        `inserted:${stats.inserted} updated:${stats.updated} ` +
        `skipped:${stats.skipped} deleted:${stats.deleted} ` +
        `total:${stats.total} (${stats.durationMs}ms)`
    );

    return stats;
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

    // ── Paso 2: Service account file ─────────────────────────────────────────
    await runStep('serviceAccountFile', () => {
        if (!fs.existsSync(KEY_FILE_PATH)) {
            throw new Error(`Archivo no encontrado: ${KEY_FILE_PATH}`);
        }
        const raw = fs.readFileSync(KEY_FILE_PATH, 'utf8');
        let parsed;
        try {
            parsed = JSON.parse(raw);
        } catch {
            throw new Error(`El archivo existe pero no es JSON válido: ${KEY_FILE_PATH}`);
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
            path: KEY_FILE_PATH,
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
