// Cache local de metadata por folio (emissionDate, total original, sellerFileId).
// Defontana solo expone GetDocumentsToPay con saldo y expirationDate; para emisión
// y monto total hay que llamar GetSale folio-a-folio, lo cual es lento. Cachear
// permite que la enriquecimiento sea incremental.

import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'Olimpia';
const COL = process.env.MONGO_FOLIO_META_COLLECTION || 'folios_meta';

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
        await col.createIndex({ folio: 1, docType: 1 }, { unique: true });
        await col.createIndex({ updatedAt: -1 });
        indexesReady = true;
    }
    return col;
}

export async function upsertFolioMeta({ folio, docType, emissionDate, total, sellerFileId, clientFile }) {
    const col = await getCol();
    return col.updateOne(
        { folio, docType },
        {
            $set: {
                folio, docType,
                emissionDate: emissionDate || null,
                total: total ?? null,
                sellerFileId: sellerFileId || null,
                clientFile: clientFile || null,
                updatedAt: new Date()
            },
            $setOnInsert: { createdAt: new Date() }
        },
        { upsert: true }
    );
}

export async function getFolioMetaMap(pairs) {
    if (!pairs.length) return new Map();
    const col = await getCol();
    const q = { $or: pairs.map(({ folio, docType }) => ({ folio, docType })) };
    const docs = await col.find(q).toArray();
    const map = new Map();
    for (const d of docs) map.set(`${d.docType}:${d.folio}`, d);
    return map;
}

export async function getMissingFolios(pairs) {
    const map = await getFolioMetaMap(pairs);
    return pairs.filter(({ folio, docType }) => !map.has(`${docType}:${folio}`));
}
