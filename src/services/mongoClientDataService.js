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
 * Genera las 3 variantes posibles del RUT chileno para tolerar inconsistencias
 * de formato en la planilla del cliente:
 *   1. Con puntos y guión:  20.133.444-5
 *   2. Sin puntos, con guión: 20133444-5
 *   3. Sin puntos ni guión:   201334445
 *
 * Acepta cualquier input (con o sin formato) y devuelve las 3 representaciones
 * deduplicadas. Si el input es inválido (< 2 dígitos), devuelve [].
 *
 * @param {string} rut
 * @returns {string[]}
 */
function generateRutVariants(rut) {
    const cleaned = String(rut || '').replace(/[^0-9kK]/g, '').toUpperCase();
    if (cleaned.length < 2) return [];
    const body = cleaned.slice(0, -1);
    const verifier = cleaned.slice(-1);
    const withDotsAndDash = body.replace(/\B(?=(\d{3})+(?!\d))/g, '.') + '-' + verifier;
    const withDashOnly = body + '-' + verifier;
    const noFormatting = body + verifier;
    return Array.from(new Set([withDotsAndDash, withDashOnly, noFormatting]));
}

/**
 * Busca todos los registros de cliente que coincidan con el RUT,
 * tolerando las 3 variantes de formato (con/sin puntos, con/sin guión).
 * Esto cubre el caso de RUTs no normalizados en la planilla del cliente.
 *
 * @param {string} rut - RUT en cualquier formato.
 * @returns {Promise<object[]>}
 */
export async function getClientsByRut(rut) {
    const variants = generateRutVariants(rut);
    if (variants.length === 0) return [];
    const collection = await getCollection();
    const docs = await collection.find({ RUT: { $in: variants } }).toArray();
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
