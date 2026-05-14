import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'Olimpia';
const COLLECTION_NAME = process.env.MONGO_DISPATCH_RECORDS_COLLECTION || 'dispatch_records';

let clientPromise = null;
let indexesReady = false;

async function getMongoClient() {
    if (!MONGO_URI) {
        throw new Error('MONGO_URI no esta definido en .env');
    }

    if (!clientPromise) {
        const client = new MongoClient(MONGO_URI);
        clientPromise = client.connect();
    }

    return clientPromise;
}

async function getCollection() {
    const client = await getMongoClient();
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    if (!indexesReady) {
        await Promise.all([
            collection.createIndex({ invoiceId: 1 }, { unique: true }),
            collection.createIndex({ clientRut: 1, branchNameNormalized: 1, dispatchDate: 1 }),
            collection.createIndex({ createdAt: -1 })
        ]);
        indexesReady = true;
    }

    return collection;
}

// Idempotencia: devuelve el documento previo si ya se registró este invoiceId.
async function findDispatchRecordByInvoiceId(invoiceId) {
    if (!invoiceId) {
        return null;
    }
    const collection = await getCollection();
    return collection.findOne({ invoiceId: String(invoiceId) });
}

// Busca registros previos de la misma sucursal para la misma fecha.
// Se usa para decidir si disparar alerta. excludeInvoiceId permite ignorar
// el registro recién insertado al evaluar colisiones (devuelve solo los OTROS).
async function findDispatchCollisions({
    clientRut,
    branchNameNormalized,
    dispatchDate,
    excludeInvoiceId = null
}) {
    if (!clientRut || !branchNameNormalized || !dispatchDate) {
        return [];
    }

    const collection = await getCollection();
    const query = {
        clientRut: String(clientRut),
        branchNameNormalized: String(branchNameNormalized),
        dispatchDate: String(dispatchDate)
    };
    if (excludeInvoiceId) {
        query.invoiceId = { $ne: String(excludeInvoiceId) };
    }

    return collection.find(query).sort({ createdAt: 1 }).toArray();
}

// Inserta el dispatch_record agregando createdAt. Maneja duplicate-key (11000)
// devolviendo { inserted: false, duplicate: true } sin lanzar.
async function insertDispatchRecord(record) {
    if (!record || !record.invoiceId) {
        throw new Error('record.invoiceId es requerido para insertar dispatch_record');
    }

    const collection = await getCollection();
    const doc = {
        ...record,
        createdAt: new Date()
    };

    try {
        const result = await collection.insertOne(doc);
        return { inserted: true, duplicate: false, insertedId: result.insertedId, doc };
    } catch (error) {
        if (error?.code === 11000) {
            return { inserted: false, duplicate: true, insertedId: null, doc: null };
        }
        throw error;
    }
}

export {
    findDispatchRecordByInvoiceId,
    findDispatchCollisions,
    insertDispatchRecord
};
