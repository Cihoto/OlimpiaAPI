import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'Olimpia';
const COLLECTION_NAME = process.env.MONGO_MANUAL_OC_SUBMIT_LOCK_COLLECTION || 'manual_oc_submit_locks';
const DEFAULT_TTL_SECONDS = Number.parseInt(process.env.MANUAL_OC_SUBMIT_LOCK_TTL_SECONDS || '600', 10);

let clientPromise = null;
let indexesReady = false;

function resolveTtlSeconds(rawTtlSeconds) {
    const parsed = Number.parseInt(String(rawTtlSeconds ?? ''), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return Number.isFinite(DEFAULT_TTL_SECONDS) && DEFAULT_TTL_SECONDS > 0
            ? DEFAULT_TTL_SECONDS
            : 600;
    }
    return parsed;
}

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
        await collection.createIndex({ sourceClientCode: 1, detectedOrderNumber: 1 }, { unique: true });
        await collection.createIndex({ expiresAt: 1 });
        indexesReady = true;
    }

    return collection;
}

async function acquireManualOcSubmitLock({
    sourceClientCode,
    detectedOrderNumber,
    ownerToken,
    ttlSeconds
}) {
    const safeSourceClientCode = String(sourceClientCode || '').trim().toUpperCase();
    const safeDetectedOrderNumber = String(detectedOrderNumber || '').trim().toUpperCase();
    const safeOwnerToken = String(ownerToken || '').trim();
    if (!safeSourceClientCode || !safeDetectedOrderNumber || !safeOwnerToken) {
        return {
            ok: false,
            reason: 'invalid_lock_input'
        };
    }

    const collection = await getCollection();
    const now = new Date();
    const ttl = resolveTtlSeconds(ttlSeconds);
    const expiresAt = new Date(now.getTime() + ttl * 1000);

    try {
        const result = await collection.findOneAndUpdate(
            {
                sourceClientCode: safeSourceClientCode,
                detectedOrderNumber: safeDetectedOrderNumber,
                $or: [
                    { expiresAt: { $lte: now } },
                    { ownerToken: safeOwnerToken }
                ]
            },
            {
                $set: {
                    sourceClientCode: safeSourceClientCode,
                    detectedOrderNumber: safeDetectedOrderNumber,
                    ownerToken: safeOwnerToken,
                    acquiredAt: now,
                    expiresAt
                }
            },
            {
                upsert: true,
                returnDocument: 'after'
            }
        );

        return {
            ok: true,
            lock: result || null,
            expiresAt: expiresAt.toISOString()
        };
    } catch (error) {
        const duplicateKey = Number(error?.code) === 11000;
        if (duplicateKey) {
            const existing = await collection.findOne({
                sourceClientCode: safeSourceClientCode,
                detectedOrderNumber: safeDetectedOrderNumber
            });

            return {
                ok: false,
                reason: 'locked_by_other_request',
                ownerToken: existing?.ownerToken || null,
                expiresAt: existing?.expiresAt ? new Date(existing.expiresAt).toISOString() : null
            };
        }
        throw error;
    }
}

async function releaseManualOcSubmitLock({
    sourceClientCode,
    detectedOrderNumber,
    ownerToken
}) {
    const safeSourceClientCode = String(sourceClientCode || '').trim().toUpperCase();
    const safeDetectedOrderNumber = String(detectedOrderNumber || '').trim().toUpperCase();
    const safeOwnerToken = String(ownerToken || '').trim();
    if (!safeSourceClientCode || !safeDetectedOrderNumber || !safeOwnerToken) {
        return {
            released: false,
            reason: 'invalid_lock_input'
        };
    }

    const collection = await getCollection();
    const result = await collection.deleteOne({
        sourceClientCode: safeSourceClientCode,
        detectedOrderNumber: safeDetectedOrderNumber,
        ownerToken: safeOwnerToken
    });

    return {
        released: result.deletedCount > 0
    };
}

export {
    acquireManualOcSubmitLock,
    releaseManualOcSubmitLock
};
