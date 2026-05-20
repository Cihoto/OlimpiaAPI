// GET-only. Recorre la API Defontana en busca de datos para calcular
// margen operacional = facturación - costo de venta. NO escribe nada.
//
// Uso:
//   node src/tools/exploreMarginEndpoints.js [yyyy-mm] [yyyy-mm]
// Ejemplo: node src/tools/exploreMarginEndpoints.js 2026-04 2026-05

import 'dotenv/config';
import fs from 'fs';
import path from 'path';

const PROD_BASE = 'https://api.defontana.com/api/';
const REP_BASE  = 'https://replapi.defontana.com/api/';

const months = (process.argv[2] && process.argv[3])
    ? [process.argv[2], process.argv[3]]
    : ['2026-04', '2026-05'];

function monthRange(yyyymm) {
    const [y, m] = yyyymm.split('-').map(Number);
    const last = new Date(y, m, 0).getDate();
    return {
        ini: `${y}-${String(m).padStart(2,'0')}-01`,
        fin: `${y}-${String(m).padStart(2,'0')}-${String(last).padStart(2,'0')}`,
        iniDDMM: `01-${String(m).padStart(2,'0')}-${y}`,
        finDDMM: `${String(last).padStart(2,'0')}-${String(m).padStart(2,'0')}-${y}`
    };
}

async function auth(base) {
    const url = `${base}Auth?client=${process.env.ID_CLIENTE}&company=${process.env.ID_EMPRESA}&user=${process.env.ID_USUARIO}&password=${process.env.PASSWORD}`;
    const r = await fetch(url, { method: 'GET', headers: { 'accept': 'text/plain' } });
    const text = await r.text();
    let body;
    try { body = JSON.parse(text); } catch { body = text; }
    return { ok: r.ok, status: r.status, body, token: body?.access_token || null };
}

async function get(base, pathAndQuery, apiKey) {
    const t0 = Date.now();
    const r = await fetch(base + pathAndQuery, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` }
    });
    const text = await r.text();
    let body;
    try { body = JSON.parse(text); } catch { body = text; }
    return { status: r.status, ms: Date.now() - t0, ok: r.ok, body };
}

function summarize(body) {
    if (!body || typeof body !== 'object') return { type: typeof body, preview: String(body).slice(0, 200) };
    const out = { keys: Object.keys(body).slice(0, 20) };
    if ('success' in body) out.success = body.success;
    if ('message' in body) out.message = body.message;
    if ('exceptionMessage' in body) out.exceptionMessage = body.exceptionMessage;
    if ('totalItems' in body) out.totalItems = body.totalItems;
    for (const k of ['saleList', 'documents', 'purchaseList', 'list', 'voucherList', 'accounts', 'plan']) {
        if (Array.isArray(body[k])) {
            out[`${k}.length`] = body[k].length;
            out[`${k}[0]`] = body[k][0];
            break;
        }
    }
    return out;
}

async function fullPaginate(base, apiKey, urlBuilder, listKey, { maxPages = 1000 } = {}) {
    const all = [];
    let total = null;
    for (let page = 1; page <= maxPages; page++) {
        const url = urlBuilder(page);
        const { body, status } = await get(base, url, apiKey);
        if (!body?.success) return { success: false, status, message: body?.message || body?.exceptionMessage || 'fail', collected: all, total };
        if (total === null) total = body.totalItems ?? null;
        const items = body[listKey] || [];
        all.push(...items);
        if (!items.length) break;
        if (total !== null && all.length >= total) break;
        if (items.length < 1) break;
        // pequeño throttle
        await new Promise(r => setTimeout(r, 120));
    }
    return { success: true, total, collected: all };
}

function fmtCLP(n) {
    if (!Number.isFinite(n)) return String(n);
    return Math.round(n).toLocaleString('es-CL');
}

async function exploreEnv(envName, base) {
    console.log(`\n############## ${envName} (${base}) ##############`);
    const a = await auth(base);
    console.log(`Auth: HTTP ${a.status}  success=${a.body?.success}  token=${a.token ? 'OK' : 'NULL'}`);
    if (!a.token) return { envName, base, auth: a, error: 'no-token' };
    const apiKey = a.token;
    const result = { envName, base, auth: { ok: true }, months: {} };

    // ----- 1) Plan de cuentas: buscamos "Costo de venta" y "Ventas" -----
    console.log('\n--- Accounting/GetAccountPlan ---');
    const plan = await get(base, 'Accounting/GetAccountPlan', apiKey);
    console.log(`HTTP ${plan.status}  ms=${plan.ms}`);
    const planKeys = Object.keys(plan.body || {}).slice(0, 10);
    console.log('keys:', planKeys);
    const accountsList = plan.body?.plan || plan.body?.accountPlan || plan.body?.accounts || plan.body;
    let costAccounts = [];
    let saleAccounts = [];
    function walk(node, depth = 0) {
        if (!node) return;
        if (Array.isArray(node)) return node.forEach(n => walk(n, depth));
        const desc = String(node.description || node.name || node.title || '').toLowerCase();
        const code = node.code || node.account || node.id || node.accountNumber || '';
        if (desc.includes('costo') && (desc.includes('venta') || desc.includes('mercader'))) {
            costAccounts.push({ code, desc: node.description || node.name });
        }
        if ((desc.includes('venta') || desc.includes('ingreso')) && !desc.includes('costo')) {
            saleAccounts.push({ code, desc: node.description || node.name });
        }
        for (const v of Object.values(node)) {
            if (v && typeof v === 'object') walk(v, depth + 1);
        }
    }
    walk(accountsList);
    console.log(`Account plan: ~${costAccounts.length} cuentas "costo de venta/mercaderías", ~${saleAccounts.length} cuentas "venta/ingresos"`);
    if (costAccounts.length) console.log('  costo (primeras 5):', costAccounts.slice(0, 5));
    if (saleAccounts.length) console.log('  ventas (primeras 5):', saleAccounts.slice(0, 5));
    result.accountPlanSummary = {
        topKeys: planKeys,
        costAccountsSample: costAccounts.slice(0, 10),
        saleAccountsSample: saleAccounts.slice(0, 10)
    };

    // ----- 2) Por mes: GetSalebyDate (facturación) + Purchase/List (compras) -----
    for (const ym of months) {
        const { ini, fin, iniDDMM, finDDMM } = monthRange(ym);
        console.log(`\n--- ${ym} (${ini} a ${fin}) ---`);
        const mo = { ini, fin };

        // Pruebo formato yyyy-MM-dd primero
        const saleProbe = await get(base, `Sale/GetSalebyDate?initialDate=${ini}&endingDate=${fin}&status=0&itemsPerPage=10&pageNumber=1`, apiKey);
        console.log(`  GetSalebyDate (yyyy-mm-dd): HTTP ${saleProbe.status} success=${saleProbe.body?.success} totalItems=${saleProbe.body?.totalItems} msg=${saleProbe.body?.message || ''}`);
        let dateFmtSale = ini;
        let dateFmtSaleFin = fin;
        if (!saleProbe.body?.success) {
            const alt = await get(base, `Sale/GetSalebyDate?initialDate=${iniDDMM}&endingDate=${finDDMM}&status=0&itemsPerPage=10&pageNumber=1`, apiKey);
            console.log(`  GetSalebyDate (dd-mm-yyyy): HTTP ${alt.status} success=${alt.body?.success} totalItems=${alt.body?.totalItems} msg=${alt.body?.message || ''}`);
            if (alt.body?.success) { dateFmtSale = iniDDMM; dateFmtSaleFin = finDDMM; saleProbe.body = alt.body; }
        }
        mo.saleProbe = { keys: Object.keys(saleProbe.body || {}), totalItems: saleProbe.body?.totalItems, success: saleProbe.body?.success, message: saleProbe.body?.message };

        if (saleProbe.body?.success) {
            const sl = saleProbe.body.saleList || saleProbe.body.list || saleProbe.body.documents || [];
            mo.saleSampleItemKeys = sl[0] ? Object.keys(sl[0]) : null;
            mo.saleSample = sl[0] || null;

            const items = saleProbe.body.totalItems || 0;
            if (items > 0) {
                const pageSize = 100;
                console.log(`  Paginando ventas (totalItems=${items})...`);
                const paged = await fullPaginate(base, apiKey,
                    p => `Sale/GetSalebyDate?initialDate=${dateFmtSale}&endingDate=${dateFmtSaleFin}&status=0&itemsPerPage=${pageSize}&pageNumber=${p}`,
                    'saleList');
                if (paged.success) {
                    mo.totalSales = paged.collected.length;
                    // Sumar afecto, exento, neto, iva, total y costos posibles
                    const sumKeys = ['total', 'netAmount', 'affectableTotal', 'totalAmount', 'exemptTotal', 'iva', 'discountAmount'];
                    const sums = {};
                    for (const it of paged.collected) {
                        for (const k of sumKeys) {
                            if (typeof it[k] === 'number') sums[k] = (sums[k] || 0) + it[k];
                        }
                    }
                    mo.sumsByField = sums;
                    console.log(`  Ventas paginadas: ${paged.collected.length} docs. Sums:`, Object.fromEntries(Object.entries(sums).map(([k,v]) => [k, fmtCLP(v)])));
                    // Sample doc types
                    const byType = {};
                    for (const it of paged.collected) {
                        const t = it.documentType || it.docType || it.type || '?';
                        byType[t] = (byType[t] || 0) + 1;
                    }
                    mo.docTypeCounts = byType;
                } else {
                    console.log('  Paginación falló:', paged.message);
                    mo.salePagFail = paged.message;
                }
            }
        }

        // ----- Compras del mes -----
        const purProbe = await get(base, `Purchase/List?StartDate=${ini}&FinishDate=${fin}&ItemsPerPage=10&Page=0`, apiKey);
        console.log(`  Purchase/List (yyyy-mm-dd): HTTP ${purProbe.status} success=${purProbe.body?.success} totalItems=${purProbe.body?.totalItems} msg=${purProbe.body?.message || ''}`);
        let pFmtIni = ini, pFmtFin = fin;
        if (!purProbe.body?.success) {
            const alt = await get(base, `Purchase/List?StartDate=${iniDDMM}&FinishDate=${finDDMM}&ItemsPerPage=10&Page=0`, apiKey);
            console.log(`  Purchase/List (dd-mm-yyyy): HTTP ${alt.status} success=${alt.body?.success} totalItems=${alt.body?.totalItems} msg=${alt.body?.message || ''}`);
            if (alt.body?.success) { pFmtIni = iniDDMM; pFmtFin = finDDMM; purProbe.body = alt.body; }
        }
        mo.purchaseProbe = { keys: Object.keys(purProbe.body || {}), totalItems: purProbe.body?.totalItems, success: purProbe.body?.success, message: purProbe.body?.message };

        if (purProbe.body?.success) {
            const pl = purProbe.body.purchaseList || purProbe.body.list || purProbe.body.documents || [];
            mo.purchaseSampleItemKeys = pl[0] ? Object.keys(pl[0]) : null;
            mo.purchaseSample = pl[0] || null;
            const items = purProbe.body.totalItems || 0;
            if (items > 0) {
                const pageSize = 50; // recomendado <= 10 según swagger pero subo a 50 para ir más rápido
                console.log(`  Paginando compras (totalItems=${items})...`);
                const paged = await fullPaginate(base, apiKey,
                    p => `Purchase/List?StartDate=${pFmtIni}&FinishDate=${pFmtFin}&ItemsPerPage=${pageSize}&Page=${p - 1}`,
                    'purchaseList');
                if (paged.success) {
                    mo.totalPurchases = paged.collected.length;
                    const sumKeys = ['total', 'netAmount', 'affectableTotal', 'totalAmount', 'exemptTotal', 'iva'];
                    const sums = {};
                    for (const it of paged.collected) {
                        for (const k of sumKeys) {
                            if (typeof it[k] === 'number') sums[k] = (sums[k] || 0) + it[k];
                        }
                    }
                    mo.sumsByFieldPurchases = sums;
                    console.log(`  Compras paginadas: ${paged.collected.length} docs. Sums:`, Object.fromEntries(Object.entries(sums).map(([k,v]) => [k, fmtCLP(v)])));
                }
            }
        }

        // ----- Sample: una factura completa con GetSale para ver si trae costo por línea -----
        const oneInvoice = mo.saleSample;
        if (oneInvoice) {
            const docType = oneInvoice.documentType || oneInvoice.docType || 'FVAELECT';
            const folio = oneInvoice.number || oneInvoice.folio || oneInvoice.folioNumber || oneInvoice.documentNumber;
            if (docType && folio) {
                const s = await get(base, `Sale/GetSale?documentType=${encodeURIComponent(docType)}&number=${folio}`, apiKey);
                console.log(`  GetSale(${docType}/${folio}) HTTP ${s.status} success=${s.body?.success}`);
                const sale = s.body?.['0'] || s.body?.sale || s.body;
                if (sale && typeof sale === 'object') {
                    const detail = sale.detail || sale.details || sale.products || sale.lineItems || sale.lineDetail || sale.items;
                    if (Array.isArray(detail) && detail[0]) {
                        const li = detail[0];
                        mo.lineItemKeys = Object.keys(li);
                        mo.lineItemSample = li;
                        const costFields = Object.keys(li).filter(k => /cost|costo/i.test(k));
                        console.log(`  GetSale línea[0] tiene ${detail.length} items. Cost-related fields: ${costFields.join(', ') || '(ninguno)'}`);
                    } else {
                        console.log(`  GetSale sin detalle (keys top-level: ${Object.keys(sale).slice(0,15).join(', ')})`);
                    }
                }
            }
        }

        result.months[ym] = mo;
    }

    return result;
}

async function main() {
    console.log(`Defontana margin explorer — meses: ${months.join(', ')}`);
    const out = { startedAt: new Date().toISOString(), months };
    try {
        out.prod = await exploreEnv('PROD', PROD_BASE);
    } catch (e) {
        out.prod = { error: e.message };
    }
    try {
        out.rep = await exploreEnv('REP (test)', REP_BASE);
    } catch (e) {
        out.rep = { error: e.message };
    }
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const file = path.resolve(process.cwd(), `tmp/margin_explore_${stamp}.json`);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify(out, null, 2));
    console.log(`\nDump completo: ${file}`);

    // Resumen rápido al final
    if (out.prod?.months) {
        console.log('\n=========== RESUMEN PROD ===========');
        for (const ym of months) {
            const m = out.prod.months[ym];
            if (!m) continue;
            const fac = m.sumsByField || {};
            const cmp = m.sumsByFieldPurchases || {};
            const facTotal = fac.netAmount ?? fac.affectableTotal ?? fac.total ?? null;
            const cmpTotal = cmp.netAmount ?? cmp.affectableTotal ?? cmp.total ?? null;
            console.log(`${ym}:`);
            console.log(`  Ventas: ${m.totalSales ?? '?'} docs, neto≈${fmtCLP(facTotal)}, total≈${fmtCLP(fac.total)}`);
            console.log(`  Compras: ${m.totalPurchases ?? '?'} docs, neto≈${fmtCLP(cmpTotal)}, total≈${fmtCLP(cmp.total)}`);
            if (Number.isFinite(facTotal) && Number.isFinite(cmpTotal)) {
                const margin = facTotal - cmpTotal;
                console.log(`  Margen NAIVE (ventas neto - compras neto): ${fmtCLP(margin)} (${(margin/facTotal*100).toFixed(1)}%)`);
                console.log('  ⚠ Esto es referencial: compras del mes ≠ COGS si hay stock diferido.');
            }
        }
    }
}

main().catch(e => { console.error('FATAL', e); process.exit(1); });
