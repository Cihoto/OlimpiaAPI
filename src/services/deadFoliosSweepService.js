// Servicio reusable que detecta folios muertos vía GetXMLDocumentBase64.success.
// Se puede invocar desde:
//   - Endpoint HTTP (on-demand desde UI)
//   - Cron job (Render scheduled)
//   - Script standalone
//
// Mantiene estado en memoria del progreso para que el front pueda hacer polling.

import { upsertFolioStatus, getDeadFolioSet } from './mongoDeadFolios.js';

const SALE_BASE = (process.env.SALE_API_URL || 'https://api.defontana.com/api/Sale/').replace(/\/+$/, '/');

let isRunning = false;
let progress = { stage: 'idle', total: 0, done: 0, deadFound: 0, transientErrors: 0 };

export function getDeadSweepStatus() {
    return { isRunning, progress };
}

async function getJSON(url, apiKey) {
    const r = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' } });
    const text = await r.text();
    try { return { ok: r.ok, status: r.status, body: JSON.parse(text) }; }
    catch { return { ok: r.ok, status: r.status, body: text }; }
}

async function checkXmlOk(apiKey, docType, folio) {
    const url = `${SALE_BASE}GetXMLDocumentBase64?documentType=${encodeURIComponent(docType)}&number=${folio}`;
    try {
        const { body } = await getJSON(url, apiKey);
        return body?.success === true;
    } catch {
        return null;
    }
}

async function parallel(items, concurrency, worker) {
    let idx = 0;
    const runners = Array.from({ length: concurrency }, async () => {
        while (idx < items.length) {
            const i = idx++;
            await worker(items[i], i);
        }
    });
    await Promise.all(runners);
}

/**
 * Sweep XML SII sobre pares (folio, docType).
 * @param {string} apiKey
 * @param {Array<{folio:number,docType:string}>} pairs
 * @param {object} [opts]
 * @param {number} [opts.concurrency=10]
 * @param {boolean} [opts.skipKnownDead=true] saltarse folios ya marcados muertos
 */
export async function sweepDeadFolios(apiKey, pairs, opts = {}) {
    if (isRunning) return { skipped: true, reason: 'already_running' };
    isRunning = true;

    const concurrency = opts.concurrency ?? 10;
    const skipKnown = opts.skipKnownDead !== false;

    progress = { stage: 'preparing', total: pairs.length, done: 0, deadFound: 0, transientErrors: 0 };

    try {
        const knownDead = skipKnown ? await getDeadFolioSet() : new Set();
        const pendientes = pairs.filter(p => !knownDead.has(`${p.docType}:${p.folio}`));
        progress.total = pendientes.length;
        progress.stage = 'scanning';

        await parallel(pendientes, concurrency, async (p) => {
            const ok = await checkXmlOk(apiKey, p.docType, p.folio);
            progress.done += 1;
            if (ok === false) {
                progress.deadFound += 1;
                await upsertFolioStatus({
                    folio: p.folio,
                    docType: p.docType,
                    classification: 'muerto',
                    xmlOk: false,
                    verifiedAt: new Date(),
                    verifiedBy: opts.source || 'sweep-service'
                });
            } else if (ok === null) {
                progress.transientErrors += 1;
            }
        });

        progress.stage = 'done';
        return { success: true, ...progress };
    } catch (error) {
        progress.stage = 'error';
        console.error('[deadSweep] error:', error);
        return { success: false, error: error.message, ...progress };
    } finally {
        isRunning = false;
    }
}

/**
 * Conveniencia: corre sweep sobre los folios únicos del último snapshot.
 */
export async function sweepDeadFromLatestSnapshot(apiKey, opts = {}) {
    const { getLatestSnapshot } = await import('./mongoCobranzaSnapshots.js');
    const snap = await getLatestSnapshot({ type: 'morosas' });
    if (!snap) return { success: false, error: 'no_snapshot' };
    const seen = new Set();
    const pairs = [];
    for (const r of (snap.rows || [])) {
        const k = `${r.docType}:${r.folio}`;
        if (seen.has(k)) continue;
        seen.add(k);
        pairs.push({ folio: r.folio, docType: r.docType });
    }
    return sweepDeadFolios(apiKey, pairs, opts);
}
