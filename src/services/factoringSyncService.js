// Batch que sincroniza cesiones de factoring desde Defontana al cache Mongo.
// Estrategia:
//   1. GetVoucherList?VoucherType=TRASPASOFACTORING&FromDate=...&ToDate=... (paginado)
//   2. Por cada voucher → GetVoucher para leer las líneas
//   3. De cada línea con documentType+documentNumber+credit>0 → registra cesión

import {
    recordFactoringCession,
    saveSyncStatus,
    getSyncStatus,
    clearAllFactoring,
    getKnownVoucherKeys
} from './mongoFactoringCache.js';

const ACC_BASE = (process.env.ACCOUNTING_API_URL_PROD || 'https://api.defontana.com/api/Accounting/').replace(/\/+$/, '/');
const VOUCHER_TYPE = 'TRASPASOFACTORING';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

let isRunning = false;
let progress = { stage: 'idle', total: 0, done: 0, errors: 0, cesionesEncontradas: 0 };

export function getFactoringSyncStatus() {
    return { isRunning, progress };
}

async function getJSON(url, apiKey) {
    const r = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' } });
    const text = await r.text();
    try { return { ok: r.ok, status: r.status, body: JSON.parse(text) }; }
    catch { return { ok: r.ok, status: r.status, body: text }; }
}

// Extrae empresa de factoring y RUT desde el gloss del voucher.
// Ejemplo de gloss: "Traspaso deuda Factoring: X CAPITAL SpA, Rut : 77078244-9, Fecha Cesion : 06-04-2026"
function parseGloss(gloss) {
    if (!gloss) return { company: null, companyRut: null };
    const companyMatch = /Factoring\s*:\s*([^,]+?)\s*,/i.exec(gloss);
    const rutMatch = /Rut\s*:\s*([\d.\-Kk]+)/i.exec(gloss);
    return {
        company: companyMatch ? companyMatch[1].trim() : null,
        companyRut: rutMatch ? rutMatch[1].trim() : null
    };
}

/**
 * @param {string} apiKey Bearer Defontana
 * @param {object} opts
 * @param {string} [opts.from] YYYY-MM-DD (default: hace 365 días)
 * @param {string} [opts.to] YYYY-MM-DD (default: hoy)
 * @param {boolean} [opts.fullReset] Si true, borra el cache antes
 */
export async function syncFactoring(apiKey, opts = {}) {
    if (isRunning) return { skipped: true, reason: 'already_running' };
    isRunning = true;

    const today = new Date();
    const yearAgo = new Date(); yearAgo.setFullYear(yearAgo.getFullYear() - 1);
    const from = opts.from || yearAgo.toISOString().substring(0, 10);
    const to = opts.to || today.toISOString().substring(0, 10);
    const fullReset = !!opts.fullReset;

    progress = { stage: 'listing', total: 0, done: 0, errors: 0, cesionesEncontradas: 0, skipped: 0, from, to };

    try {
        if (fullReset) await clearAllFactoring();

        // Sync incremental: si NO es fullReset, evitamos pegar GetVoucher para vouchers
        // que ya tenemos cacheados (matchea por fiscalYear:voucherNumber).
        const knownKeys = fullReset ? new Set() : await getKnownVoucherKeys();

        // 1) Listar todos los vouchers TRASPASOFACTORING en el rango
        const vouchers = [];
        let page = 0;
        const itemsPerPage = 100;
        while (true) {
            const url = `${ACC_BASE}GetVoucherList?VoucherType=${encodeURIComponent(VOUCHER_TYPE)}&FromDate=${from}&ToDate=${to}&ItemsPerPage=${itemsPerPage}&Page=${page}`;
            const { body } = await getJSON(url, apiKey);
            if (!body?.success && body?.exceptionMessage) {
                // Algunos endpoints devuelven 200 con success:false. Pero también el body puede no traer 'success'.
                console.warn('[factoring] respuesta sin success:', body?.message);
            }
            const list = body?.vouchers || body?.items || body?.voucherList || [];
            if (list.length === 0) break;
            vouchers.push(...list);
            if (list.length < itemsPerPage) break;
            page += 1;
            if (page > 50) break; // safety
        }

        progress.total = vouchers.length;
        progress.stage = 'detailing';

        // 2) Para cada voucher leer detail (saltando los ya cacheados)
        for (let i = 0; i < vouchers.length; i++) {
            const v = vouchers[i];
            try {
                const vt = v.voucherType || v.type || VOUCHER_TYPE;
                const num = v.number ?? v.voucherNumber ?? v.id;
                const fy = v.fiscalYear || v.year || (new Date(v.date || Date.now())).getUTCFullYear();

                // Skip incremental: el voucher ya está en el cache.
                if (knownKeys.has(`${fy}:${num}`)) {
                    progress.skipped += 1;
                    progress.done = i + 1;
                    continue;
                }

                const url = `${ACC_BASE}GetVoucher?VoucherType=${encodeURIComponent(vt)}&Number=${num}&FiscalYear=${fy}`;
                const { body } = await getJSON(url, apiKey);
                if (!body?.header || !body?.detail) { progress.errors += 1; continue; }

                const gloss = body.header.comment || body.header.gloss || v.gloss || '';
                const { company, companyRut } = parseGloss(gloss);
                const date = body.header.date || v.date;

                // Buscar líneas con documentNumber y monto en credit (lo cedido baja la deuda del cliente)
                for (const line of body.detail) {
                    if (!line.documentType || !line.documentNumber) continue;
                    const amount = line.credit || 0; // monto cedido
                    if (amount <= 0) continue;
                    await recordFactoringCession({
                        folio: line.documentNumber,
                        docType: line.documentType,
                        voucherNumber: body.header.number,
                        fiscalYear: body.header.fiscalYear,
                        date,
                        amount,
                        company,
                        companyRut
                    });
                    progress.cesionesEncontradas += 1;
                }
            } catch (err) {
                progress.errors += 1;
                console.warn('[factoring] err voucher:', err.message);
            }
            progress.done = i + 1;
            await sleep(80);
        }

        await saveSyncStatus({
            lastSyncAt: new Date(),
            from, to,
            vouchersProcessed: vouchers.length,
            vouchersSkipped: progress.skipped,
            cesionesEncontradas: progress.cesionesEncontradas,
            errors: progress.errors
        });

        progress.stage = 'done';
        return { success: true, ...progress };
    } catch (error) {
        progress.stage = 'error';
        console.error('[factoring] sync error:', error);
        return { success: false, error: error.message, ...progress };
    } finally {
        isRunning = false;
    }
}

export { getSyncStatus };
