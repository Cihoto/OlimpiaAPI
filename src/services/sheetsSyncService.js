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
