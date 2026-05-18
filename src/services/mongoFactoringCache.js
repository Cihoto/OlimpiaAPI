// Cache de cesiones de factoring por folio.
// Guarda cuánto de cada folio fue cedido a una empresa de factoring,
// con qué fecha y a qué empresa. Se llena con un batch que pega a
// /api/Accounting/GetVoucherList?VoucherType=TRASPASOFACTORING.

import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'Olimpia';
const COL = process.env.MONGO_FACTORING_COLLECTION || 'folio_factoring';
const META_COL = process.env.MONGO_FACTORING_META_COLLECTION || 'folio_factoring_meta';

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

async function getMeta() {
    const c = await getClient();
    return c.db(DB_NAME).collection(META_COL);
}

// Upsert: si el folio ya existe, suma la cesión nueva a su array y recalcula totales.
export async function recordFactoringCession({ folio, docType, voucherNumber, fiscalYear, date, amount, company, companyRut }) {
    if (!folio || !docType || !amount) return null;
    const col = await getCol();
    const cession = { voucherNumber, fiscalYear, date: date ? new Date(date) : null, amount, company: company || null, companyRut: companyRut || null };

    // Buscar si ya existe esta cesión (mismo voucher) para no duplicar
    const existing = await col.findOne({ folio, docType, 'cessions.voucherNumber': voucherNumber, 'cessions.fiscalYear': fiscalYear });
    if (existing) {
        // ya cargada, no hacer nada
        return { duplicated: true };
    }

    return col.updateOne(
        { folio, docType },
        {
            $push: { cessions: cession },
            $inc: { totalCedido: amount },
            $set: {
                lastCessionDate: cession.date,
                factoringCompany: company || null,
                factoringRut: companyRut || null,
                updatedAt: new Date()
            },
            $setOnInsert: { createdAt: new Date() }
        },
        { upsert: true }
    );
}

export async function getFactoringMap(pairs) {
    if (!pairs || pairs.length === 0) return new Map();
    const col = await getCol();
    const q = { $or: pairs.map(({ folio, docType }) => ({ folio, docType })) };
    const docs = await col.find(q).toArray();
    const map = new Map();
    for (const d of docs) map.set(`${d.docType}:${d.folio}`, d);
    return map;
}

export async function listAllFactoring() {
    const col = await getCol();
    return col.find({}).toArray();
}

// Devuelve un Set con los vouchers ya procesados (clave "fiscalYear:voucherNumber").
// Sirve para sync incremental — el batch puede saltarse GetVoucher si ya conoce el voucher.
export async function getKnownVoucherKeys() {
    const col = await getCol();
    const docs = await col.find({}, { projection: { cessions: 1 } }).toArray();
    const keys = new Set();
    for (const d of docs) {
        for (const c of d.cessions || []) {
            if (c.voucherNumber != null && c.fiscalYear != null) {
                keys.add(`${c.fiscalYear}:${c.voucherNumber}`);
            }
        }
    }
    return keys;
}

export async function getSyncStatus() {
    const meta = await getMeta();
    return meta.findOne({ _id: 'factoring_sync' });
}

export async function saveSyncStatus(payload) {
    const meta = await getMeta();
    return meta.updateOne(
        { _id: 'factoring_sync' },
        { $set: { ...payload, updatedAt: new Date() } },
        { upsert: true }
    );
}

export async function clearAllFactoring() {
    const col = await getCol();
    await col.deleteMany({});
}
