// Cache local de clientes de Defontana. Refresh manual o por TTL.
// Permite que la Vista 1 no espere los 1.5 minutos del getAllClients en cada generación.

import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'Olimpia';
const COLLECTION = process.env.MONGO_CLIENTS_CACHE_COLLECTION || 'clients_cache';
const META_COLLECTION = process.env.MONGO_CLIENTS_CACHE_META_COLLECTION || 'clients_cache_meta';

const TTL_HOURS = 24; // cache se considera fresco por 24h

let clientPromise = null;
let indexesReady = false;

async function getClient() {
    if (!clientPromise) clientPromise = new MongoClient(MONGO_URI).connect();
    return clientPromise;
}

async function getCol() {
    const c = await getClient();
    const col = c.db(DB_NAME).collection(COLLECTION);
    if (!indexesReady) {
        await col.createIndex({ fileID: 1 }, { unique: true });
        await col.createIndex({ legalCode: 1 });
        await col.createIndex({ lastSyncedAt: -1 });
        indexesReady = true;
    }
    return col;
}

async function getMeta() {
    const c = await getClient();
    return c.db(DB_NAME).collection(META_COLLECTION);
}

export async function getCacheStatus() {
    const meta = await getMeta();
    const doc = await meta.findOne({ _id: 'clients_sync' });
    if (!doc) return { synced: false, lastSyncedAt: null, count: 0, isStale: true };
    const ageMs = Date.now() - new Date(doc.lastSyncedAt).getTime();
    const isStale = ageMs > TTL_HOURS * 60 * 60 * 1000;
    return {
        synced: true,
        lastSyncedAt: doc.lastSyncedAt,
        count: doc.count || 0,
        ageMs,
        isStale
    };
}

export async function saveClients(clients) {
    const col = await getCol();
    const meta = await getMeta();
    const now = new Date();

    // Dedup por RUT normalizado (sin puntos/guiones) — la paginación de Defontana
    // puede devolver duplicados con distinto fileID pero mismo legalCode.
    const normalize = (s) => String(s || '').replace(/[.\-\s]/g, '').toUpperCase();
    const dedup = new Map();
    for (const c of clients) {
        const key = normalize(c.legalCode || c.fileID);
        if (!key) continue;
        if (!dedup.has(key)) dedup.set(key, c);
    }
    const uniqueClients = Array.from(dedup.values());

    // Limpiar y reinsertar (el set de clientes activos puede cambiar)
    await col.deleteMany({});
    if (uniqueClients.length > 0) {
        const docs = uniqueClients.map(c => ({
            fileID: c.fileID || c.legalCode,
            legalCode: c.legalCode || c.fileID,
            name: c.name || null,
            business: c.business || null,
            sellerID: c.sellerID || c.salesmanID || null,
            paymentID: c.paymentID || null,
            email: c.email || null,
            city: c.city || null,
            state: c.state || null,
            district: c.district || null,
            lastSyncedAt: now
        }));
        await col.insertMany(docs, { ordered: false });
    }

    await meta.updateOne(
        { _id: 'clients_sync' },
        { $set: { lastSyncedAt: now, count: clients.length } },
        { upsert: true }
    );

    return { count: clients.length, lastSyncedAt: now };
}

export async function listClients({ excludeRuts = [] } = {}) {
    const col = await getCol();
    const q = {};
    if (excludeRuts.length > 0) {
        q.$and = [
            { legalCode: { $nin: excludeRuts } },
            { fileID: { $nin: excludeRuts } }
        ];
    }
    return col.find(q).sort({ name: 1 }).toArray();
}
