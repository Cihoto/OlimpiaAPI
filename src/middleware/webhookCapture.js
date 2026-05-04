/**
 * webhookCapture.js
 *
 * Middleware Express que registra cada request entrante en la colección
 * Olimpia._webhookCaptures (DB separada de OlimpiaClients). NO modifica
 * ni el request ni la respuesta — solo observa.
 *
 * Aplicar SOLO en rutas específicas que se quieran auditar
 * (ej. POST /helpers/sync-knowledgebase). No usar como middleware global.
 *
 * Falla suave: si Mongo no responde, loguea y deja pasar el request.
 */

import { ensureTTLIndexes, getWebhookCapturesCollection } from '../services/auditStore.js';

const REDACTED = '***REDACTED***';
const SENSITIVE_HEADERS = new Set(['authorization', 'cookie', 'x-api-key']);

function redactHeaders(headers) {
    const out = {};
    for (const [key, value] of Object.entries(headers || {})) {
        out[key] = SENSITIVE_HEADERS.has(key.toLowerCase()) ? REDACTED : value;
    }
    return out;
}

function computeBodySize(body) {
    if (body == null) return 0;
    if (typeof body === 'string') return Buffer.byteLength(body);
    if (Buffer.isBuffer(body)) return body.length;
    try {
        return Buffer.byteLength(JSON.stringify(body));
    } catch {
        return 0;
    }
}

export function webhookCapture(req, res, next) {
    const startedAt = new Date();

    const requestSnapshot = {
        method: req.method,
        url: req.originalUrl || req.url,
        headers: redactHeaders(req.headers),
        bodyType: typeof req.body,
        bodySize: computeBodySize(req.body),
        body: req.body ?? null
    };

    res.on('finish', async () => {
        try {
            await ensureTTLIndexes();
            const col = await getWebhookCapturesCollection();
            await col.insertOne({
                createdAt: startedAt,
                durationMs: Date.now() - startedAt.getTime(),
                request: requestSnapshot,
                response: { status: res.statusCode }
            });
        } catch (err) {
            console.error('[webhookCapture] No se pudo guardar el capture:', err.message);
        }
    });

    next();
}
