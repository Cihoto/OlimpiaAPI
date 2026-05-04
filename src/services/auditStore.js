/**
 * auditStore.js
 *
 * Acceso a la database `Olimpia` (separada de `chatbot`) donde se guardan
 * snapshots del Sheet y capturas de webhook para auditoría.
 *
 * Colecciones:
 *   _sheetSyncSnapshots  — un doc por sync, con todas las filas del Sheet
 *   _webhookCaptures     — un doc por request al endpoint de sync
 *
 * Ambas colecciones tienen un TTL index sobre `createdAt` que borra
 * automáticamente docs con más de 90 días.
 */

import { MongoClient } from 'mongodb';

const TTL_DAYS = 90;
const TTL_SECONDS = TTL_DAYS * 24 * 60 * 60;

let _client = null;
let _ttlEnsured = false;

async function getClient() {
    if (!_client) {
        const uri = process.env.MONGO_URI;
        if (!uri) throw new Error('MONGO_URI no está definido en .env');
        _client = new MongoClient(uri);
        await _client.connect();
    }
    return _client;
}

function getAuditDbName() {
    return process.env.MONGO_AUDIT_DB_NAME || 'Olimpia';
}

export async function getSnapshotsCollection() {
    const client = await getClient();
    const colName = process.env.MONGO_SNAPSHOTS_COLLECTION || '_sheetSyncSnapshots';
    return client.db(getAuditDbName()).collection(colName);
}

export async function getWebhookCapturesCollection() {
    const client = await getClient();
    const colName = process.env.MONGO_WEBHOOK_CAPTURES_COLLECTION || '_webhookCaptures';
    return client.db(getAuditDbName()).collection(colName);
}

/**
 * Crea (idempotente) los TTL indexes sobre `createdAt`. Se ejecuta una vez
 * por proceso; si ya están creados, MongoDB es idempotente.
 */
export async function ensureTTLIndexes() {
    if (_ttlEnsured) return;
    try {
        const snapshots = await getSnapshotsCollection();
        const captures = await getWebhookCapturesCollection();
        await Promise.all([
            snapshots.createIndex({ createdAt: 1 }, { expireAfterSeconds: TTL_SECONDS }),
            captures.createIndex({ createdAt: 1 }, { expireAfterSeconds: TTL_SECONDS })
        ]);
        _ttlEnsured = true;
    } catch (err) {
        console.error('[auditStore] No se pudieron crear los TTL indexes:', err.message);
        // No interrumpe — escrituras siguen funcionando, solo sin auto-purge.
    }
}
