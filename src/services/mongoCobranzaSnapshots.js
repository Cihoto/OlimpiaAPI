// Snapshots completos del reporte de morosas. Guarda rows + summary con timestamp
// para iterar rápido sin tener que re-llamar Defontana cliente por cliente.

import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'Olimpia';
const COL = process.env.MONGO_COBRANZA_SNAPSHOTS_COLLECTION || 'cobranza_snapshots';

let clientPromise = null;
let indexesReady = false;

async function getClient() {
    if (!clientPromise) clientPromise = new MongoClient(MONGO_URI).connect();
    return clientPromise;
}

async function getCol() {
    const c = await getClient();
    const col = c.db(DB_NAME).collection(COL);
    if (!indexesReady) {
        await col.createIndex({ generatedAt: -1 });
        await col.createIndex({ type: 1, generatedAt: -1 });
        indexesReady = true;
    }
    return col;
}

export async function saveSnapshot({ type = 'morosas', rows, summary, generatedBy = null, source = 'manual', meta = {} }) {
    const col = await getCol();
    const doc = {
        type,
        rows,
        summary,
        generatedAt: new Date(),
        generatedBy,
        source,
        meta,
        rowsCount: rows.length
    };
    const res = await col.insertOne(doc);
    return { insertedId: res.insertedId, generatedAt: doc.generatedAt };
}

export async function getLatestSnapshot({ type = 'morosas' } = {}) {
    const col = await getCol();
    return col.findOne({ type }, { sort: { generatedAt: -1 } });
}

export async function listSnapshots({ type = 'morosas', limit = 30 } = {}) {
    const col = await getCol();
    return col
        .find({ type }, { projection: { rows: 0 } }) // sin rows para el listado
        .sort({ generatedAt: -1 })
        .limit(limit)
        .toArray();
}

export async function getSnapshotById(id) {
    const col = await getCol();
    const { ObjectId } = await import('mongodb');
    return col.findOne({ _id: new ObjectId(id) });
}
