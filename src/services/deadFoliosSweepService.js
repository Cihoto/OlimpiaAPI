// Servicio reusable que detecta folios muertos vía GetXMLDocumentBase64.success.
// Se puede invocar desde:
//   - Endpoint HTTP (on-demand desde UI)
//   - Cron job (Render scheduled)
//   - Script standalone
//
// Mantiene estado en memoria del progreso para que el front pueda hacer polling.

import { upsertFolioStatus, getDeadFolioSet, getDeadFolios, reviveFolios } from './mongoDeadFolios.js';

const SALE_BASE = (process.env.SALE_API_URL || 'https://api.defontana.com/api/Sale/').replace(/\/+$/, '/');

// Período de gracia: una factura recién emitida puede no tener todavía su XML
// en el SII al momento del sweep. GetXMLDocumentBase64 devuelve success:false y
// la marcábamos "muerta" de forma permanente (falso positivo). Durante este
// período NO se marca muerto ningún folio aunque Defontana diga success:false.
const GRACE_DAYS = Number(process.env.COBRANZA_DEAD_GRACE_DAYS || 7);

let isRunning = false;
let progress = { stage: 'idle', total: 0, done: 0, deadFound: 0, transientErrors: 0 };

function isWithinGraceDays(emissionDate, graceDays = GRACE_DAYS) {
    if (!emissionDate) return false; // sin fecha no podemos proteger; se evalúa normal
    const em = new Date(emissionDate);
    if (Number.isNaN(em.getTime())) return false;
    const ageDays = (Date.now() - em.getTime()) / (1000 * 60 * 60 * 24);
    return ageDays >= 0 && ageDays < graceDays;
}

export function getDeadSweepStatus() {
    return { isRunning, progress };
}

async function getJSON(url, apiKey) {
    const r = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' } });
    const text = await r.text();
    try { return { ok: r.ok, status: r.status, body: JSON.parse(text) }; }
    catch { return { ok: r.ok, status: r.status, body: text }; }
}

// Devuelve:
//   true  -> XML llegó al SII (folio vivo)
//   false -> Defontana respondió OK y afirma explícitamente que NO hay XML (folio muerto)
//   null  -> respuesta no concluyente (error HTTP, timeout, HTML, body no-JSON, success ausente)
//
// CRÍTICO: nunca devolver `false` ante un error de transporte/servidor. Un 500/429/HTML
// de Defontana NO significa que la factura esté muerta; significa que no sabemos. Tratar
// "el endpoint falló" como "muerto confirmado" corrompe el reporte de forma persistente
// (incidente 2026-06-08: un glitch del endpoint XML marcó 453 facturas vivas como muertas).
async function checkXmlOk(apiKey, docType, folio) {
    const url = `${SALE_BASE}GetXMLDocumentBase64?documentType=${encodeURIComponent(docType)}&number=${folio}`;
    try {
        const { ok, body } = await getJSON(url, apiKey);
        // Solo confiamos en una respuesta HTTP 2xx con JSON parseado.
        if (!ok || typeof body !== 'object' || body === null) return null; // transitorio
        if (body.success === true) return true;   // vivo
        if (body.success === false) return false;  // muerto confirmado por Defontana
        return null; // forma inesperada -> no arriesgar
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
    // Circuit breaker: una corrida sana detecta unos pocos muertos. Si de golpe
    // "muere" una fracción/cantidad implausible del batch, es un fallo upstream
    // (token sin scope, caída del endpoint XML), NO una avalancha real de facturas
    // muertas. En ese caso abortamos y NO persistimos nada.
    const abortRate = opts.abortRate ?? 0.20;   // >20% del batch
    const abortAbs = opts.abortAbs ?? 40;       // o >40 muertos absolutos

    progress = { stage: 'preparing', total: pairs.length, done: 0, deadFound: 0, transientErrors: 0 };

    try {
        const knownDead = skipKnown ? await getDeadFolioSet() : new Set();
        const pendientes = pairs.filter(p => !knownDead.has(`${p.docType}:${p.folio}`));
        progress.total = pendientes.length;
        progress.stage = 'scanning';

        // Fase 1: escanear y RECOLECTAR candidatos a muerto (no persistir todavía).
        const deadCandidates = [];
        progress.protectedByGrace = 0;
        await parallel(pendientes, concurrency, async (p) => {
            const ok = await checkXmlOk(apiKey, p.docType, p.folio);
            progress.done += 1;
            if (ok === false) {
                // Período de gracia: no matar facturas recién emitidas cuyo XML
                // aún puede no estar en el SII (causa del reflood de falsos positivos).
                if (isWithinGraceDays(p.emissionDate)) {
                    progress.protectedByGrace += 1;
                    return;
                }
                progress.deadFound += 1;
                deadCandidates.push(p);
            } else if (ok === null) {
                progress.transientErrors += 1;
            }
        });

        // Fase 2: sanity check antes de persistir.
        const n = pendientes.length;
        const tooMany = deadCandidates.length > abortAbs && (n > 0 && deadCandidates.length / n > abortRate);
        if (tooMany) {
            progress.stage = 'aborted';
            console.error(
                `[deadSweep] ABORTADO: ${deadCandidates.length}/${n} folios saldrían "muertos" ` +
                `(${(100 * deadCandidates.length / n).toFixed(0)}% > umbral). Probable fallo upstream de ` +
                `GetXMLDocumentBase64 (transientErrors=${progress.transientErrors}). No se marcó ningún folio.`
            );
            return {
                success: false,
                aborted: true,
                reason: 'implausible_dead_rate',
                deadCandidates: deadCandidates.length,
                ...progress
            };
        }

        // Fase 3: persistir los muertos confirmados.
        for (const p of deadCandidates) {
            await upsertFolioStatus({
                folio: p.folio,
                docType: p.docType,
                classification: 'muerto',
                xmlOk: false,
                verifiedAt: new Date(),
                verifiedBy: opts.source || 'sweep-service'
            });
        }

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
        pairs.push({ folio: r.folio, docType: r.docType, emissionDate: r.emissionDate });
    }
    return sweepDeadFolios(apiKey, pairs, opts);
}

/**
 * Self-healing: re-verifica los folios marcados 'muerto' y des-marca los que
 * resultan vivos (GetXMLDocumentBase64.success === true). Convierte el flag
 * 'muerto' de permanente a auto-corregible: si un folio se marcó por un glitch
 * transitorio o antes de que su XML estuviera en el SII, vuelve solo a CxC.
 * LEE de Defontana (GET) + ESCRIBE solo en nuestra Mongo (nunca en Defontana).
 * @param {string} apiKey
 * @param {object} [opts]
 * @param {number} [opts.concurrency=10]
 * @param {string} [opts.source]
 * @param {number} [opts.limit] re-verificar a lo más N (los más recientes por verifiedAt)
 */
export async function reviveResurrectedFolios(apiKey, opts = {}) {
    const concurrency = opts.concurrency ?? 10;
    let dead = await getDeadFolios();
    if (opts.limit && dead.length > opts.limit) {
        dead = dead.slice()
            .sort((a, b) => new Date(b.verifiedAt || 0) - new Date(a.verifiedAt || 0))
            .slice(0, opts.limit);
    }
    let checked = 0, alive = 0, transient = 0;
    const toRevive = [];
    await parallel(dead, concurrency, async (d) => {
        const ok = await checkXmlOk(apiKey, d.docType, d.folio);
        checked += 1;
        if (ok === true) { alive += 1; toRevive.push({ folio: d.folio, docType: d.docType }); }
        else if (ok === null) { transient += 1; }
    });
    const { revived } = await reviveFolios(toRevive, { by: opts.source || 'revive-service' });
    return { success: true, deadTotal: dead.length, checked, alive, transient, revived };
}
