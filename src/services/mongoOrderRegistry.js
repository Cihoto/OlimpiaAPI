import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'Olimpia';
const COLLECTION_NAME = 'init';

let clientPromise = null;
let indexesReady = false;

async function getMongoClient() {
    if (!MONGO_URI) {
        throw new Error('MONGO_URI no está definido en .env');
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
        await collection.createIndex({ clientId: 1, ocNumber: 1 }, { unique: true });
        indexesReady = true;
    }

    return collection;
}

async function findProcessedKeyLogisticsOrder({ clientId, ocNumber }) {
    if (!clientId || !ocNumber) {
        return null;
    }
    const collection = await getCollection();
    return collection.findOne({ clientId, ocNumber });
}

async function insertProcessedKeyLogisticsOrder({
    clientId,
    ocNumber,
    sender,
    messageId,
    emailDate,
    attachmentFilename,
    quantities,
    source = 'keylogistics'
}) {
    if (!clientId || !ocNumber) {
        return { skipped: true, reason: 'missing_client_or_oc' };
    }

    const collection = await getCollection();

    const doc = {
        clientId,
        ocNumber,
        sender,
        messageId,
        emailDate,
        attachmentFilename,
        quantities,
        source,
        status: 'success',
        createdAt: new Date()
    };

    await collection.insertOne(doc);
    return { inserted: true };
}

export { findProcessedKeyLogisticsOrder, insertProcessedKeyLogisticsOrder };
