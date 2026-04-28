import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_CLIENTS_DB_NAME || 'chatbot';
const COLLECTION_NAME = process.env.MONGO_CLIENTS_COLLECTION || 'OlimpiaClients';

let clientPromise = null;

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
    return db.collection(COLLECTION_NAME);
}

/**
 * Busca todos los registros de cliente que coincidan con el RUT formateado (ej: "76.773.009-8").
 * @param {string} normalizedRut - RUT con formato de puntos y guión.
 * @returns {Promise<object[]>}
 */
export async function getClientsByRut(normalizedRut) {
    const collection = await getCollection();
    const docs = await collection.find({ RUT: normalizedRut }).toArray();
    return docs;
}

/**
 * Retorna todos los documentos de la colección de clientes.
 * Usado para búsquedas por dirección sin filtro de RUT.
 * @returns {Promise<object[]>}
 */
export async function getAllClients() {
    const collection = await getCollection();
    const docs = await collection.find({}).toArray();
    return docs;
}
