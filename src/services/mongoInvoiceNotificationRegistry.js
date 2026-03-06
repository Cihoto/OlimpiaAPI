import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'Olimpia';
const COLLECTION_NAME = process.env.MONGO_INVOICE_NOTIFICATION_COLLECTION || 'invoice_notifications';

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
        await collection.createIndex({ invoiceId: 1, status: 1 }, { unique: true });
        indexesReady = true;
    }

    return collection;
}

async function findSentInvoiceNotification({ invoiceId, status }) {
    if (!invoiceId || !status) {
        return null;
    }

    const collection = await getCollection();
    return collection.findOne({ invoiceId: String(invoiceId), status: String(status) });
}

async function saveSentInvoiceNotification({
    invoiceId,
    rutCliente,
    messageId,
    status,
    fromEmail,
    recipientEmail
}) {
    if (!invoiceId || !status) {
        throw new Error('invoiceId y status son requeridos para guardar idempotencia');
    }

    const collection = await getCollection();
    const doc = {
        invoiceId: String(invoiceId),
        rutCliente: rutCliente ? String(rutCliente) : '',
        sentAt: new Date(),
        messageId: messageId ? String(messageId) : null,
        status: String(status),
        fromEmail: fromEmail ? String(fromEmail) : '',
        recipientEmail: recipientEmail ? String(recipientEmail) : ''
    };

    try {
        await collection.insertOne(doc);
        return { inserted: true, duplicate: false };
    } catch (error) {
        if (error?.code === 11000) {
            return { inserted: false, duplicate: true };
        }
        throw error;
    }
}

export {
    findSentInvoiceNotification,
    saveSentInvoiceNotification
};
