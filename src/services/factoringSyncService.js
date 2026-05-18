// Batch que sincroniza cesiones de factoring desde Defontana al cache Mongo.
//
// Estrategia v2:
//   1. Lista voucher types a procesar (env var FACTORING_VOUCHER_TYPES — CSV).
//      Default: "TRASPASOFACTORING". Si Olimpia usa otros tipos para registrar
//      cesiones, agregarlos al env (ej "TRASPASOFACTORING,CONFIRMING,CESIONFACTORING").
//   2. Para cada tipo: GetVoucherList paginado en el rango from..to.
//   3. Por cada voucher → GetVoucher para leer las líneas.
//   4. De cada línea con documentType+documentNumber+credit>0:
//      - Parsear company y companyRut desde la gloss del header.
//      - Si FACTORING_REAL_RUTS está definido y companyRut NO está en la lista,
//        descartar la cesión (sirve para filtrar confirming, intercompany, etc.
//        que no son factoring formal: RENDIC, TOTTUS, etc.).
//      - Si pasa el filtro → grabar en cache.

import {
    recordFactoringCession,
    saveSyncStatus,
    getSyncStatus,
    clearAllFactoring,
    getKnownVoucherKeys
} from './mongoFactoringCache.js';

const ACC_BASE = (process.env.ACCOUNTING_API_URL_PROD || 'https://api.defontana.com/api/Accounting/').replace(/\/+$/, '/');

// Voucher types a procesar. Default: solo TRASPASOFACTORING. El usuario puede
// agregar más via env FACTORING_VOUCHER_TYPES="TRASPASOFACTORING,CONFIRMING".
function getVoucherTypes(override) {
    if (Array.isArray(override) && override.length) return override;
    const raw = process.env.FACTORING_VOUCHER_TYPES || 'TRASPASOFACTORING';
    return raw.split(',').map(s => s.trim()).filter(Boolean);
}

// Whitelist de RUTs de empresas de factoring REALES. Si la cesión va a un RUT
// que NO está acá, se descarta (no es factoring formal). Vacío = aceptar todo
// (modo legacy). Se configura via env FACTORING_REAL_RUTS (CSV).
function getRealFactoringRuts() {
    const raw = process.env.FACTORING_REAL_RUTS;
    if (!raw) return null; // null = no filtrar
    return new Set(raw.split(',').map(s => normalizeRut(s)).filter(Boolean));
}

function normalizeRut(rut) {
    if (!rut) return '';
    return String(rut).replace(/[\.\s]/g, '').toUpperCase();
}

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
// Glosas observadas (Defontana es inconsistente con `:` después de "Factoring"):
//   "Traspaso deuda Factoring: X CAPITAL SpA, Rut : 77078244-9, Fecha Cesion : 06-04-2026"
//   "Traspaso deuda Factoring Bci Factoring, Rut  96720830-2, Fecha Cesion  02-12-2025"
//   "Traspaso deuda Factoring RENDIC HERMANOS SA , Rut  81537600-5, Fecha Cesion  04-09-2025"
//   "Traspaso deuda Factoring : Bice Factoring S.A. Rut : 76562786-9"  (sin coma)
//   "FACTORING 22-01 X CAPITAL"  (libre, sin estructura)
function parseGloss(gloss) {
    if (!gloss) return { company: null, companyRut: null };

    // RUT: aceptar con/sin puntos, con/sin dos puntos previos.
    const rutMatch = /Rut\s*:?\s*([\d\.]{7,12}-?[\dKk])/i.exec(gloss);
    const companyRut = rutMatch ? rutMatch[1].trim() : null;

    let company = null;

    // Estrategia universal: "Factoring [opcional :] NOMBRE_EMPRESA , Rut ..."
    // La empresa es todo lo que esté entre "Factoring" y la siguiente coma o " Rut".
    let m = /Factoring\s*:?\s*(.+?)\s*,\s*Rut/i.exec(gloss);
    if (m) company = m[1];

    // Variante: sin coma entre empresa y Rut
    if (!company) {
        m = /Factoring\s*:?\s*(.+?)\s+Rut\s*:?/i.exec(gloss);
        if (m) company = m[1];
    }

    // Variante: solo "Factoring : EMPRESA" (final de línea)
    if (!company) {
        m = /Factoring\s*:\s*([^,\n]+?)\s*$/i.exec(gloss);
        if (m) company = m[1];
    }

    // Variante: "Cesión a EMPRESA Rut"
    if (!company) {
        m = /Cesi[óo]n\s+a\s+(.+?)\s+Rut/i.exec(gloss);
        if (m) company = m[1];
    }

    // Limpiar y descartar matches obviamente inválidos.
    if (company) {
        company = company.trim();
        // No queremos que se capture la palabra "deuda" o similar (cuando la gloss
        // empieza por "Traspaso deuda Factoring NOMBRE" y la regex captura "deuda").
        if (/^(deuda|deudas?|cesion|traspaso|por|al?|del?)$/i.test(company)) {
            company = null;
        }
    }
    return { company: company || null, companyRut };
}

async function fetchVoucherList(apiKey, voucherType, from, to) {
    const vouchers = [];
    let page = 0;
    const itemsPerPage = 100;
    while (true) {
        const url = `${ACC_BASE}GetVoucherList?VoucherType=${encodeURIComponent(voucherType)}&FromDate=${from}&ToDate=${to}&ItemsPerPage=${itemsPerPage}&Page=${page}`;
        const { body } = await getJSON(url, apiKey);
        if (body?.success === false && body?.exceptionMessage) {
            console.warn(`[factoring] tipo "${voucherType}" devolvió error: ${body.exceptionMessage}`);
            break;
        }
        const list = body?.vouchers || body?.items || body?.voucherList || [];
        if (list.length === 0) break;
        // Marcar cada voucher con su tipo origen (sirve cuando procesamos varios tipos)
        for (const v of list) v.__sourceType = voucherType;
        vouchers.push(...list);
        if (list.length < itemsPerPage) break;
        page += 1;
        if (page > 100) break; // safety
    }
    return vouchers;
}

/**
 * @param {string} apiKey Bearer Defontana
 * @param {object} opts
 * @param {string}   [opts.from] YYYY-MM-DD (default: hace 365 días)
 * @param {string}   [opts.to] YYYY-MM-DD (default: hoy)
 * @param {boolean}  [opts.fullReset] Si true, borra el cache antes
 * @param {string[]} [opts.voucherTypes] override de voucher types
 * @param {string[]} [opts.realFactoringRuts] override de whitelist de RUTs (null = no filtrar)
 */
export async function syncFactoring(apiKey, opts = {}) {
    if (isRunning) return { skipped: true, reason: 'already_running' };
    isRunning = true;

    const today = new Date();
    const yearAgo = new Date(); yearAgo.setFullYear(yearAgo.getFullYear() - 1);
    const from = opts.from || yearAgo.toISOString().substring(0, 10);
    const to = opts.to || today.toISOString().substring(0, 10);
    const fullReset = !!opts.fullReset;
    const voucherTypes = getVoucherTypes(opts.voucherTypes);
    // Whitelist: si pasaste null/undefined explícitamente y no hay env, no filtra.
    const whitelistRuts = opts.realFactoringRuts !== undefined
        ? (opts.realFactoringRuts ? new Set(opts.realFactoringRuts.map(normalizeRut)) : null)
        : getRealFactoringRuts();

    progress = {
        stage: 'listing', total: 0, done: 0, errors: 0,
        cesionesEncontradas: 0, descartadasFiltro: 0, skipped: 0,
        from, to, voucherTypes,
        whitelistEnabled: !!whitelistRuts
    };

    try {
        if (fullReset) await clearAllFactoring();

        const knownKeys = fullReset ? new Set() : await getKnownVoucherKeys();

        // 1) Recolectar vouchers de TODOS los tipos configurados.
        const vouchers = [];
        for (const type of voucherTypes) {
            const fromType = await fetchVoucherList(apiKey, type, from, to);
            console.log(`[factoring] tipo "${type}": ${fromType.length} vouchers`);
            vouchers.push(...fromType);
        }

        progress.total = vouchers.length;
        progress.stage = 'detailing';

        // 2) Procesar cada voucher
        for (let i = 0; i < vouchers.length; i++) {
            const v = vouchers[i];
            try {
                const vt = v.voucherType || v.type || v.__sourceType || voucherTypes[0];
                const num = v.number ?? v.voucherNumber ?? v.id;
                const fy = v.fiscalYear || v.year || (new Date(v.date || Date.now())).getUTCFullYear();

                if (knownKeys.has(`${fy}:${num}:${vt}`)) {
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

                // Filtro whitelist: si está habilitado, solo aceptamos cesiones a
                // RUTs de factores reales. Sirve para descartar RENDIC, TOTTUS, etc.
                if (whitelistRuts) {
                    const rutKey = normalizeRut(companyRut);
                    if (!rutKey || !whitelistRuts.has(rutKey)) {
                        progress.descartadasFiltro += 1;
                        progress.done = i + 1;
                        await sleep(80);
                        continue;
                    }
                }

                for (const line of body.detail) {
                    if (!line.documentType || !line.documentNumber) continue;
                    const amount = line.credit || 0;
                    if (amount <= 0) continue;
                    await recordFactoringCession({
                        folio: line.documentNumber,
                        docType: line.documentType,
                        voucherNumber: body.header.number,
                        fiscalYear: body.header.fiscalYear,
                        date,
                        amount,
                        company,
                        companyRut,
                        glossRaw: gloss || null,
                        voucherType: vt
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
            voucherTypes,
            whitelistEnabled: !!whitelistRuts,
            vouchersProcessed: vouchers.length,
            vouchersSkipped: progress.skipped,
            cesionesEncontradas: progress.cesionesEncontradas,
            descartadasFiltro: progress.descartadasFiltro,
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
