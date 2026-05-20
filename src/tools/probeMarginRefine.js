// GET-only. Refina el probe de margen:
// - Desglosa ventas por documentType (factura vs guía vs nota crédito/débito)
// - Toma una factura FVAELECT real y mira si sus líneas traen costo unitario
// - Ajusta el parser de Purchase/List (clave "data")
// - Consulta GetAccountAnalisys sobre las cuentas de costo de venta

import 'dotenv/config';
import fs from 'fs';
import path from 'path';

const BASE = 'https://api.defontana.com/api/';

const months = (process.argv[2] && process.argv[3])
    ? [process.argv[2], process.argv[3]]
    : ['2026-04', '2026-05'];

function monthRange(yyyymm) {
    const [y, m] = yyyymm.split('-').map(Number);
    const last = new Date(y, m, 0).getDate();
    return {
        ini: `${y}-${String(m).padStart(2,'0')}-01`,
        fin: `${y}-${String(m).padStart(2,'0')}-${String(last).padStart(2,'0')}`
    };
}

async function auth() {
    const url = `${BASE}Auth?client=${process.env.ID_CLIENTE}&company=${process.env.ID_EMPRESA}&user=${process.env.ID_USUARIO}&password=${process.env.PASSWORD}`;
    const r = await fetch(url, { method: 'GET', headers: { 'accept': 'text/plain' } });
    const j = await r.json();
    if (!j.success) throw new Error('Auth failed: ' + JSON.stringify(j));
    return j.access_token;
}

async function get(pathAndQuery, apiKey) {
    const r = await fetch(BASE + pathAndQuery, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` }
    });
    const text = await r.text();
    try { return { status: r.status, body: JSON.parse(text) }; } catch { return { status: r.status, body: text }; }
}

function fmtCLP(n) {
    if (!Number.isFinite(n)) return String(n);
    return Math.round(n).toLocaleString('es-CL');
}

async function paginateSales(apiKey, ini, fin) {
    const all = [];
    let total = null;
    for (let page = 1; page <= 200; page++) {
        const { body } = await get(`Sale/GetSalebyDate?initialDate=${ini}&endingDate=${fin}&status=0&itemsPerPage=100&pageNumber=${page}`, apiKey);
        if (!body?.success) break;
        total = total ?? body.totalItems;
        const list = body.saleList || [];
        all.push(...list);
        if (!list.length || all.length >= (body.totalItems || 0)) break;
        await new Promise(r => setTimeout(r, 100));
    }
    return { total, list: all };
}

async function paginatePurchases(apiKey, ini, fin) {
    const all = [];
    let total = null;
    let probeKeys = null;
    let probeBody = null;
    for (let page = 0; page < 200; page++) {
        const { body } = await get(`Purchase/List?StartDate=${ini}&FinishDate=${fin}&ItemsPerPage=50&Page=${page}`, apiKey);
        if (!body?.success) break;
        if (page === 0) { probeKeys = Object.keys(body); probeBody = body; }
        total = total ?? (body.totalItems ?? body.totalRecords ?? null);
        // Defontana devuelve los items bajo "data" en Purchase/List — buscamos
        // cualquier propiedad cuyo valor sea array (la lista real).
        let list = null;
        for (const k of ['data', 'purchaseList', 'list', 'items', 'documents']) {
            if (Array.isArray(body[k])) { list = body[k]; break; }
        }
        if (!list) {
            for (const [k, v] of Object.entries(body)) {
                if (Array.isArray(v) && v.length && typeof v[0] === 'object') { list = v; break; }
            }
        }
        if (!list) {
            return { total, list: all, probeKeys, probeBody };
        }
        all.push(...list);
        if (!list.length) break;
        if (total && all.length >= total) break;
        if (list.length < 50) break;
        await new Promise(r => setTimeout(r, 100));
    }
    return { total, list: all, probeKeys, probeBody };
}

async function getAccountMovements(apiKey, accountCode, fromDate, toDate) {
    // Probaremos varios parámetros - el swagger sólo lista "Account" pero
    // la búsqueda real usualmente acepta período/fechas. Hacemos un par de variantes.
    const variants = [
        `Accounting/GetAccountAnalisys?Account=${accountCode}`,
        `Accounting/GetAccountAnalisys?Account=${accountCode}&FromDate=${fromDate}&ToDate=${toDate}`,
        `Accounting/GetAccountAnalisys?Account=${accountCode}&fromDate=${fromDate}&toDate=${toDate}`
    ];
    const tries = [];
    for (const v of variants) {
        const r = await get(v, apiKey);
        tries.push({ url: v, status: r.status, success: r.body?.success, msg: r.body?.message, keys: r.body && typeof r.body === 'object' ? Object.keys(r.body).slice(0, 10) : [], sample: r.body });
        if (r.body?.success) break;
    }
    return tries;
}

async function main() {
    const apiKey = await auth();
    console.log('Auth OK');

    const out = { startedAt: new Date().toISOString(), months: {} };

    for (const ym of months) {
        const { ini, fin } = monthRange(ym);
        console.log(`\n=== ${ym} (${ini} → ${fin}) ===`);
        const mo = {};

        // ----- Ventas -----
        const { total: tSales, list: sales } = await paginateSales(apiKey, ini, fin);
        console.log(`Ventas: ${sales.length} docs (totalItems reportado=${tSales})`);
        const byType = {};
        const sumsByType = {};
        for (const s of sales) {
            const t = s.documentType || '?';
            byType[t] = (byType[t] || 0) + 1;
            sumsByType[t] = sumsByType[t] || { total: 0, affectableTotal: 0, exemptTotal: 0, count: 0 };
            sumsByType[t].count += 1;
            sumsByType[t].total += s.total || 0;
            sumsByType[t].affectableTotal += s.affectableTotal || 0;
            sumsByType[t].exemptTotal += s.exemptTotal || 0;
        }
        mo.salesByType = sumsByType;
        console.log('Ventas por documentType:');
        for (const [t, v] of Object.entries(sumsByType).sort((a, b) => b[1].total - a[1].total)) {
            console.log(`  ${t.padEnd(10)} count=${String(v.count).padStart(4)}  neto=${fmtCLP(v.affectableTotal).padStart(15)}  total=${fmtCLP(v.total).padStart(15)}`);
        }

        // Facturación real = facturas (FVAELECT, FENAELECT, BELECT, etc.) menos notas de crédito
        const FACT_TYPES = ['FVAELECT', 'FENAELECT', 'BELECT', 'BENELECT', 'FVAEXP', 'FCEM', 'FACELECT'];
        const CN_TYPES   = ['NCVELECT', 'NCEELECT', 'NCREELECT', 'NCELECT', 'NCRELECT'];
        const factSum = { total: 0, affectableTotal: 0, exemptTotal: 0, count: 0 };
        const cnSum   = { total: 0, affectableTotal: 0, exemptTotal: 0, count: 0 };
        for (const [t, v] of Object.entries(sumsByType)) {
            if (FACT_TYPES.includes(t)) { factSum.total += v.total; factSum.affectableTotal += v.affectableTotal; factSum.exemptTotal += v.exemptTotal; factSum.count += v.count; }
            if (CN_TYPES.includes(t))   { cnSum.total += v.total; cnSum.affectableTotal += v.affectableTotal; cnSum.exemptTotal += v.exemptTotal; cnSum.count += v.count; }
        }
        mo.facturacion = { facturas: factSum, notasCredito: cnSum, netoNetoFacturado: factSum.affectableTotal - cnSum.affectableTotal };
        console.log(`\nFacturación NETA del mes (facturas - notas crédito), base AFECTO:`);
        console.log(`  Facturas:       ${factSum.count} docs, neto=${fmtCLP(factSum.affectableTotal)}, total=${fmtCLP(factSum.total)}`);
        console.log(`  Notas crédito:  ${cnSum.count} docs, neto=${fmtCLP(cnSum.affectableTotal)}, total=${fmtCLP(cnSum.total)}`);
        console.log(`  NETO FACTURADO: ${fmtCLP(factSum.affectableTotal - cnSum.affectableTotal)} (sin IVA)`);
        console.log(`  TOTAL FACT-NC:  ${fmtCLP(factSum.total - cnSum.total)} (con IVA)`);

        // ----- Sample de una factura FVAELECT para ver costo en línea -----
        const sampleFact = sales.find(s => FACT_TYPES.includes(s.documentType));
        if (sampleFact) {
            const folio = sampleFact.firstFolio || sampleFact.lastFolio || sampleFact.number;
            const dt = sampleFact.documentType;
            const r = await get(`Sale/GetSale?documentType=${encodeURIComponent(dt)}&number=${folio}`, apiKey);
            const sale = r.body?.['0'] || r.body?.sale || r.body;
            if (sale && Array.isArray(sale.details)) {
                const li = sale.details[0];
                mo.factSample = { docType: dt, folio, total: sale.total, affectableTotal: sale.affectableTotal, detailCount: sale.details.length, lineKeys: Object.keys(li), line0: li };
                const costFields = Object.keys(li).filter(k => /cost|costo/i.test(k));
                console.log(`\nMuestra de factura ${dt}/${folio}: ${sale.details.length} líneas, total=${fmtCLP(sale.total)}`);
                console.log('  Keys línea:', Object.keys(li).join(', '));
                console.log('  Campos relacionados a costo:', costFields.length ? costFields : '(ninguno → no hay COGS por línea)');
            }
        }

        // ----- Compras -----
        const { total: tPurch, list: purchases } = await paginatePurchases(apiKey, ini, fin);
        console.log(`\nCompras: ${purchases.length} docs (totalItems reportado=${tPurch})`);
        if (purchases[0]) {
            mo.purchaseSampleKeys = Object.keys(purchases[0]);
            console.log('  Keys de compra:', Object.keys(purchases[0]).slice(0, 30).join(', '));
        }
        const pSums = { total: 0, affectableTotal: 0, exemptTotal: 0, neto: 0, iva: 0, count: purchases.length };
        const pByType = {};
        for (const p of purchases) {
            for (const k of ['total', 'affectableTotal', 'exemptTotal', 'netAmount', 'neto', 'iva', 'tax']) {
                if (typeof p[k] === 'number') pSums[k === 'netAmount' ? 'neto' : k] = (pSums[k === 'netAmount' ? 'neto' : k] || 0) + p[k];
            }
            const t = p.documentType || p.documentTypeCode || p.docType || '?';
            pByType[t] = (pByType[t] || 0) + 1;
        }
        mo.purchaseSums = pSums;
        mo.purchaseByType = pByType;
        console.log('  Sums compras:', Object.fromEntries(Object.entries(pSums).map(([k, v]) => [k, fmtCLP(v)])));
        console.log('  Tipos de compra:', pByType);

        // ----- Cuentas contables de costo de venta -----
        console.log('\n--- Movimientos cuenta COSTO DE VENTAS (4110101001) ---');
        const acct = await getAccountMovements(apiKey, '4110101001', ini, fin);
        mo.accountAnalisys = acct.map(t => ({ url: t.url, status: t.status, success: t.success, msg: t.msg, keys: t.keys }));
        for (const t of acct) {
            console.log(`  ${t.url.replace(BASE, '')} → HTTP ${t.status} success=${t.success} msg=${t.msg || ''} keys=[${t.keys.join(',')}]`);
            if (t.success && t.sample) {
                // exploremos el sample
                const sample = t.sample;
                console.log('     sample top keys:', Object.keys(sample).slice(0, 15));
                if (sample.movements || sample.data || sample.items) {
                    const arr = sample.movements || sample.data || sample.items;
                    if (Array.isArray(arr)) {
                        console.log(`     array length=${arr.length}, first item keys=${Object.keys(arr[0] || {}).join(',')}`);
                        mo.accountAnalisysSample = { itemCount: arr.length, sample: arr[0] };
                    }
                }
                break;
            }
        }

        out.months[ym] = mo;
    }

    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const file = path.resolve(process.cwd(), `tmp/margin_refine_${stamp}.json`);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify(out, null, 2));
    console.log(`\nDump completo: ${file}`);

    // ===== Resumen final =====
    console.log('\n========== RESUMEN ==========');
    for (const ym of months) {
        const m = out.months[ym];
        const fact = m.facturacion;
        const pSums = m.purchaseSums;
        console.log(`\n${ym}:`);
        console.log(`  Facturación neta del mes (sin IVA, facturas - notas crédito): ${fmtCLP(fact.netoNetoFacturado)} CLP`);
        const compraNeta = pSums.affectableTotal || pSums.neto || 0;
        console.log(`  Compras netas del mes:                                        ${fmtCLP(compraNeta)} CLP`);
        if (compraNeta && fact.netoNetoFacturado) {
            const m1 = fact.netoNetoFacturado - compraNeta;
            console.log(`  Diferencia NAIVE (fact neto - compras netas):                 ${fmtCLP(m1)} (${(m1/fact.netoNetoFacturado*100).toFixed(1)}%)`);
            console.log('  ⚠ Esto NO es margen real: compras del mes ≠ COGS del mes.');
        }
    }
}

main().catch(e => { console.error('FATAL', e); process.exit(1); });
