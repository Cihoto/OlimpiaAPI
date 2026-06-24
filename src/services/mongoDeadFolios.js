// Persistencia de folios muertos detectados en Defontana.
// Evita re-escanear cada vez. Indexa por (folio, docType).

import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'Olimpia';
const COLLECTION = process.env.MONGO_DEAD_FOLIOS_COLLECTION || 'dead_folios';

let clientPromise = null;
let indexesReady = false;

async function getClient() {
    if (!clientPromise) clientPromise = new MongoClient(MONGO_URI).connect();
    return clientPromise;
}

async function getCollection() {
    const client = await getClient();
    const col = client.db(DB_NAME).collection(COLLECTION);
    if (!indexesReady) {
        await col.createIndex({ folio: 1, docType: 1 }, { unique: true });
        await col.createIndex({ classification: 1 });
        indexesReady = true;
    }
    return col;
}

export async function upsertFolioStatus(record) {
    if (!record?.folio || !record?.docType) return null;
    const col = await getCollection();
    return col.updateOne(
        { folio: record.folio, docType: record.docType },
        { $set: { ...record, updatedAt: new Date() }, $setOnInsert: { createdAt: new Date() } },
        { upsert: true }
    );
}

export async function getDeadFolios({ docType = null } = {}) {
    const col = await getCollection();
    const q = { classification: 'muerto' };
    if (docType) q.docType = docType;
    return col.find(q).toArray();
}

export async function getDeadFolioSet({ docType = null } = {}) {
    const docs = await getDeadFolios({ docType });
    return new Set(docs.map(d => `${d.docType}:${d.folio}`));
}

// Des-marca folios que estaban como 'muerto' pero resultaron vivos al re-verificar.
// NO los borra: cambia classification a 'revivido' y conserva auditoría. Como
// getDeadFolios()/getDeadFolioSet() filtran classification:'muerto', un folio
// 'revivido' deja de excluirse del reporte y vuelve a cuentas por cobrar.
// @param {Array<{folio:number,docType:string}>} pairs
export async function reviveFolios(pairs, { by = 'revive-service' } = {}) {
    if (!Array.isArray(pairs) || pairs.length === 0) return { revived: 0 };
    const col = await getCollection();
    const ops = pairs.map(p => ({
        updateOne: {
            filter: { folio: p.folio, docType: p.docType, classification: 'muerto' },
            update: {
                $set: {
                    classification: 'revivido',
                    xmlOk: true,
                    revivedAt: new Date(),
                    revivedBy: by,
                    updatedAt: new Date()
                }
            }
        }
    }));
    const res = await col.bulkWrite(ops, { ordered: false });
    return { revived: res.modifiedCount || 0 };
}

export async function getLastScannedFolio(docType) {
    const col = await getCollection();
    const last = await col.find({ docType }).sort({ folio: -1 }).limit(1).toArray();
    return last[0]?.folio || null;
}

// Timestamp del último folio verificado (cualquier classification). Sirve
// para mostrar al usuario cuándo se corrió el último sweep.
export async function getLastVerifiedAt() {
    const col = await getCollection();
    const last = await col.find({ verifiedAt: { $exists: true } }).sort({ verifiedAt: -1 }).limit(1).toArray();
    return last[0]?.verifiedAt || null;
}
