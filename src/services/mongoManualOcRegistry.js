import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'Olimpia';
const COLLECTION_NAME = process.env.MONGO_MANUAL_OC_COLLECTION || 'manual_oc_logs';

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
        await collection.createIndex({ manualOcId: 1 }, { unique: true });
        await collection.createIndex({ status: 1, createdAt: -1 });
        await collection.createIndex({ sourceClientCode: 1, createdAt: -1 });
        await collection.createIndex(
            { sourceClientCode: 1, detectedOrderNumber: 1, createdAt: -1 },
            { partialFilterExpression: { detectedOrderNumber: { $exists: true, $type: 'string' } } }
        );
        indexesReady = true;
    }

    return collection;
}

async function createManualOcRecord(doc) {
    const collection = await getCollection();
    await collection.insertOne({
        ...doc,
        createdAt: new Date(),
        updatedAt: new Date()
    });
    return { inserted: true };
}

async function findManualOcRecord(manualOcId) {
    if (!manualOcId) {
        return null;
    }
    const collection = await getCollection();
    return collection.findOne({ manualOcId: String(manualOcId) });
}

async function updateManualOcRecord(manualOcId, updates = {}) {
    if (!manualOcId) {
        throw new Error('manualOcId es requerido para actualizar registro');
    }
    const collection = await getCollection();
    const result = await collection.findOneAndUpdate(
        { manualOcId: String(manualOcId) },
        {
            $set: {
                ...updates,
                updatedAt: new Date()
            }
        },
        { returnDocument: 'after' }
    );

    return result;
}

async function appendManualOcTimeline(manualOcId, event) {
    if (!manualOcId) {
        throw new Error('manualOcId es requerido para registrar timeline');
    }
    const collection = await getCollection();
    const timelineEvent = {
        ...(event || {}),
        at: new Date().toISOString()
    };

    await collection.updateOne(
        { manualOcId: String(manualOcId) },
        {
            $push: { timeline: timelineEvent },
            $set: { updatedAt: new Date() }
        }
    );
}

async function findLatestManualOcByDetectedOrderNumber({
    sourceClientCode,
    detectedOrderNumber,
    statuses = []
}) {
    const safeSourceClientCode = String(sourceClientCode || '').trim().toUpperCase();
    const safeDetectedOrderNumber = String(detectedOrderNumber || '').trim().toUpperCase();
    if (!safeSourceClientCode || !safeDetectedOrderNumber) {
        return null;
    }

    const collection = await getCollection();
    const filter = {
        sourceClientCode: safeSourceClientCode,
        detectedOrderNumber: safeDetectedOrderNumber
    };

    if (Array.isArray(statuses) && statuses.length > 0) {
        filter.status = { $in: statuses };
    }

    return collection.find(filter).sort({ createdAt: -1 }).limit(1).next();
}

export {
    createManualOcRecord,
    findManualOcRecord,
    updateManualOcRecord,
    appendManualOcTimeline,
    findLatestManualOcByDetectedOrderNumber
};
