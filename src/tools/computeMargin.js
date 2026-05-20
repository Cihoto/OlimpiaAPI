// GET-only. Calcula margen operacional real mes a mes a través de tres vías
// independientes para triangular el número:
//
//  Vía A (FACTURACIÓN): Sale/GetSalebyDate filtrando facturas - notas crédito
//  Vía B (COSTO contable): Accounting/GetVoucherList + GetVoucher, sumando
//        débitos en cuentas de la familia 411xxxxxx (COSTOS DE VENTAS)
//  Vía C (COMPRAS): Purchase/List, total neto de documentos de compra del mes
//
// Uso: node src/tools/computeMargin.js [yyyy-mm] [yyyy-mm]

import 'dotenv/config';
import fs from 'fs';
import path from 'path';

const BASE = 'https://api.defontana.com/api/';
const months = (process.argv[2] && process.argv[3]) ? [process.argv[2], process.argv[3]] : ['2026-04', '2026-05'];

// Familia de cuentas de costo de venta detectada en el plan
const COST_ACCOUNT_PREFIXES = ['411'];  // 4110100000, 4110101000, 4110101001...
const COST_ACCOUNTS_NAMES_RE = /costo.*venta|costos.*venta/i;

function monthRange(yyyymm) {
    const [y, m] = yyyymm.split('-').map(Number);
    const last = new Date(y, m, 0).getDate();
    return { y, ini: `${y}-${String(m).padStart(2,'0')}-01`, fin: `${y}-${String(m).padStart(2,'0')}-${String(last).padStart(2,'0')}` };
}

async function auth() {
    const url = `${BASE}Auth?client=${process.env.ID_CLIENTE}&company=${process.env.ID_EMPRESA}&user=${process.env.ID_USUARIO}&password=${process.env.PASSWORD}`;
    const r = await fetch(url, { method: 'GET', headers: { 'accept': 'text/plain' } });
    const j = await r.json();
    if (!j.success) throw new Error('Auth failed');
    return j.access_token;
}

async function http(method, p, apiKey, body) {
    const r = await fetch(BASE + p, {
        method, headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
        body: body ? JSON.stringify(body) : undefined
    });
    const t = await r.text();
    try { return { status: r.status, body: JSON.parse(t) }; } catch { return { status: r.status, body: t }; }
}
const G = (p, k) => http('GET', p, k);

// Parser de strings de moneda chilena: "$ 239,828" → 239828; "$ 1.234.567" → 1234567
function parseCLP(s) {
    if (typeof s === 'number') return s;
    if (!s) return 0;
    const n = Number(String(s).replace(/[^\d-]/g, ''));
    return Number.isFinite(n) ? n : 0;
}
const fmt = n => Number.isFinite(n) ? Math.round(n).toLocaleString('es-CL') : String(n);

// ===== VÍA A: Facturación neta =====
async function calcFacturacion(apiKey, ini, fin) {
    const all = [];
    for (let p = 1; p <= 200; p++) {
        const { body } = await G(`Sale/GetSalebyDate?initialDate=${ini}&endingDate=${fin}&status=0&itemsPerPage=100&pageNumber=${p}`, apiKey);
        if (!body?.success) break;
        const list = body.saleList || [];
        all.push(...list);
        if (!list.length || all.length >= (body.totalItems || 0)) break;
        await new Promise(r => setTimeout(r, 80));
    }
    const FACT = ['FVAELECT', 'FENAELECT', 'BELECT', 'BENELECT', 'FVAEXP', 'FCEM', 'FACELECT'];
    const NC   = ['NCVELECT', 'NCEELECT', 'NCREELECT', 'NCELECT', 'NCRELECT'];
    const byType = {};
    for (const s of all) {
        const t = s.documentType || '?';
        byType[t] = byType[t] || { count: 0, neto: 0, total: 0, exento: 0 };
        byType[t].count += 1;
        byType[t].neto  += s.affectableTotal || 0;
        byType[t].total += s.total || 0;
        byType[t].exento += s.exemptTotal || 0;
    }
    const fSum = { count: 0, neto: 0, total: 0 };
    const cSum = { count: 0, neto: 0, total: 0 };
    for (const [t, v] of Object.entries(byType)) {
        if (FACT.includes(t)) { fSum.count += v.count; fSum.neto += v.neto; fSum.total += v.total; }
        if (NC.includes(t))   { cSum.count += v.count; cSum.neto += v.neto; cSum.total += v.total; }
    }
    return {
        docsTotales: all.length,
        byType,
        facturas: fSum,
        notasCredito: cSum,
        netoFacturado: fSum.neto - cSum.neto,
        totalFacturadoConIva: fSum.total - cSum.total
    };
}

// ===== VÍA C: Compras netas =====
// RUTs de factores conocidos (factoring NO es compra real, es operación financiera)
const FACTORING_RUTS = new Set([
    '77078244-9', '76562786-9', '96655860-1', '76197101-8',
    '97004000-5', '81537600-5', '78627210-6'
]);

async function calcCompras(apiKey, ini, fin) {
    const all = [];
    const PAGE_SIZE = 100;
    let total = null;
    for (let page = 0; page < 500; page++) {
        const { body } = await G(`Purchase/List?StartDate=${ini}&FinishDate=${fin}&ItemsPerPage=${PAGE_SIZE}&Page=${page}`, apiKey);
        if (!body?.success) break;
        const data = body.data || {};
        // recordsTotal sí parece ser el total absoluto; recordsFiltered es con filtros aplicados
        if (page === 0) total = data.recordsTotal ?? data.recordsFiltered ?? null;
        const list = data.data || [];
        all.push(...list);
        if (!list.length) break;
        if (total && all.length >= total) break;
        if (list.length < PAGE_SIZE) break;
        await new Promise(r => setTimeout(r, 80));
    }
    // Procesamos cada compra y separamos en categorías: mercadería real,
    // factoring/financieras (excluir), notas crédito (restar).
    const byType = {};
    const byProvider = {};
    let sumTotalConIva = 0;
    let sumExentos = 0;
    let sumAfectos = 0;
    let factoringConIva = 0;
    let ncConIva = 0;
    let realPurchasesConIva = 0;
    let realAfectos = 0;
    let realExentos = 0;
    for (const d of all) {
        const sii = String(d.siiDocumentType || '');
        const t = `${sii} ${d.documentType || ''}`.trim();
        byType[t] = byType[t] || { count: 0, total: 0 };
        const tot = parseCLP(d.documentTotal);
        byType[t].count += 1;
        byType[t].total += tot;
        sumTotalConIva += tot;
        const isExento = ['34', '41', '110', '111', '112'].includes(sii);
        if (isExento) sumExentos += tot; else sumAfectos += tot;
        // Notas crédito del proveedor (61) o nota crédito exenta (60 / 112): se restan
        const isNC = ['61', '60', '112'].includes(sii);
        const isFactoring = FACTORING_RUTS.has(d.providerLegalCode || '');
        const rut = d.providerLegalCode || '?';
        byProvider[rut] = byProvider[rut] || { name: d.providerName, count: 0, total: 0, isFactoring };
        byProvider[rut].count += 1;
        byProvider[rut].total += isNC ? -tot : tot;
        if (isFactoring) {
            factoringConIva += tot;
        } else if (isNC) {
            ncConIva += tot;
            if (isExento) realExentos -= tot; else realAfectos -= tot;
        } else {
            realPurchasesConIva += tot;
            if (isExento) realExentos += tot; else realAfectos += tot;
        }
    }
    const netoRealEstimado = Math.round(realAfectos / 1.19) + realExentos;
    const top = Object.entries(byProvider).sort((a,b) => b[1].total - a[1].total).slice(0, 20);
    return {
        docsTotales: all.length,
        totalReportado: total,
        byType,
        totalBrutoConIva: sumTotalConIva,
        factoringConIva,
        notasCreditoConIva: ncConIva,
        realPurchasesConIva,
        realAfectos,
        realExentos,
        netoRealEstimado,
        topProveedores: top
    };
}

// ===== VÍA B: COGS contable =====
async function calcCogsContable(apiKey, ini, fin, fy) {
    // Listar TODOS los vouchers del mes (paginando) — sin FiscalYear (rompe el endpoint)
    const vouchers = [];
    for (let page = 0; page < 200; page++) {
        const url = `Accounting/GetVoucherList?FromDate=${ini}&ToDate=${fin}&ItemsPerPage=100&Page=${page}`;
        const { body } = await G(url, apiKey);
        if (!body?.success) break;
        const items = body.items || [];
        vouchers.push(...items);
        if (!items.length || (body.totalItems && vouchers.length >= body.totalItems)) break;
        if (items.length < 100) break;
        await new Promise(r => setTimeout(r, 80));
    }
    console.log(`  [COGS] ${vouchers.length} vouchers a inspeccionar`);

    // Abrir cada voucher y acumular cargos en cuentas de costo de venta.
    const byAccount = {};   // { code: { debit, credit, count } }
    const byVoucherType = {}; // qué tipo de voucher aporta al COGS
    let inspected = 0;
    let withCost = 0;
    const concurrency = 8;
    let cursor = 0;
    async function worker() {
        while (cursor < vouchers.length) {
            const i = cursor++;
            const v = vouchers[i];
            const vt = v.voucherType, num = v.number, year = v.fiscalYear || fy;
            try {
                const { body } = await G(`Accounting/GetVoucher?VoucherType=${encodeURIComponent(vt)}&FiscalYear=${year}&Number=${num}`, apiKey);
                if (!body?.success) continue;
                const detail = body.detail || [];
                let touchedCost = false;
                for (const m of detail) {
                    const code = String(m.accountCode || '');
                    if (!code) continue;
                    if (COST_ACCOUNT_PREFIXES.some(pf => code.startsWith(pf))) {
                        touchedCost = true;
                        byAccount[code] = byAccount[code] || { debit: 0, credit: 0, count: 0 };
                        byAccount[code].debit  += Number(m.debit)  || 0;
                        byAccount[code].credit += Number(m.credit) || 0;
                        byAccount[code].count  += 1;
                    }
                }
                if (touchedCost) {
                    withCost += 1;
                    byVoucherType[vt] = (byVoucherType[vt] || 0) + 1;
                }
                inspected += 1;
                if (inspected % 100 === 0) console.log(`    inspected=${inspected}/${vouchers.length}  withCost=${withCost}`);
            } catch (e) {
                // ignore single voucher errors
            }
        }
    }
    await Promise.all(Array.from({ length: concurrency }, () => worker()));
    console.log(`  [COGS] inspected=${inspected}, vouchers con cuenta costo=${withCost}`);

    // Total: débitos - créditos (en la cuenta de gasto el costo va al DEBE)
    let cogs = 0;
    for (const [code, v] of Object.entries(byAccount)) {
        cogs += (v.debit - v.credit);
    }
    return {
        vouchersTotal: vouchers.length,
        vouchersInspected: inspected,
        vouchersConCostoVenta: withCost,
        vouchersConCostoPorTipo: byVoucherType,
        cuentasMovidas: byAccount,
        cogsContable: cogs
    };
}

async function main() {
    const apiKey = await auth();
    console.log('Auth OK');
    const out = { generatedAt: new Date().toISOString(), months: {} };

    for (const ym of months) {
        const { ini, fin, y } = monthRange(ym);
        console.log(`\n========== ${ym} (${ini} → ${fin}) ==========`);

        console.log('VÍA A: facturación...');
        const A = await calcFacturacion(apiKey, ini, fin);
        console.log(`  Facturas:       ${A.facturas.count} docs, neto $${fmt(A.facturas.neto)}, total $${fmt(A.facturas.total)}`);
        console.log(`  Notas crédito:  ${A.notasCredito.count} docs, neto $${fmt(A.notasCredito.neto)}, total $${fmt(A.notasCredito.total)}`);
        console.log(`  ▶ FACTURACIÓN NETA (sin IVA): $${fmt(A.netoFacturado)}  | con IVA: $${fmt(A.totalFacturadoConIva)}`);

        console.log('\nVÍA C: compras...');
        const C = await calcCompras(apiKey, ini, fin);
        console.log(`  ${C.docsTotales} documentos de compra (reportado=${C.totalReportado})`);
        console.log(`  Total bruto con IVA: $${fmt(C.totalBrutoConIva)}`);
        console.log(`    - factoring/financieras: $${fmt(C.factoringConIva)}`);
        console.log(`    - notas crédito proveedor: $${fmt(C.notasCreditoConIva)}`);
        console.log(`    = compras REALES con IVA: $${fmt(C.realPurchasesConIva)}`);
        console.log(`  ▶ COMPRAS NETAS REALES estimadas (IVA 19%): $${fmt(C.netoRealEstimado)}`);
        console.log(`  Top 10 proveedores (compras reales del mes):`);
        for (const [rut, v] of C.topProveedores.slice(0, 10)) {
            const tag = v.isFactoring ? '[FACTORING]' : '';
            console.log(`    ${rut.padEnd(15)} ${tag} ${(v.name||'').slice(0,40).padEnd(40)} $${fmt(v.total)} (${v.count})`);
        }

        console.log('\nVÍA B: COGS contable (iterando vouchers del mes)...');
        const B = await calcCogsContable(apiKey, ini, fin, y);
        console.log(`  Total vouchers: ${B.vouchersTotal} | inspeccionados: ${B.vouchersInspected} | con cargo a costo de venta: ${B.vouchersConCostoVenta}`);
        console.log(`  Tipos de voucher que registran COGS:`, B.vouchersConCostoPorTipo);
        console.log(`  Cuentas usadas:`);
        for (const [c, v] of Object.entries(B.cuentasMovidas).sort((a, b) => (b[1].debit - b[1].credit) - (a[1].debit - a[1].credit))) {
            console.log(`    ${c}: debe $${fmt(v.debit)} - haber $${fmt(v.credit)} = $${fmt(v.debit - v.credit)}  (${v.count} mov)`);
        }
        console.log(`  ▶ COGS CONTABLE NETO (debe - haber): $${fmt(B.cogsContable)}`);

        // ===== Síntesis =====
        const facNeto = A.netoFacturado;
        const cogs = B.cogsContable;
        const compras = C.netoRealEstimado;
        console.log('\n  ─── MARGEN OPERACIONAL ───');
        if (cogs > 0) {
            const margenAbs = facNeto - cogs;
            const margenPct = (margenAbs / facNeto) * 100;
            console.log(`  Vía CONTABLE (más fiel):`);
            console.log(`    Facturación neta: $${fmt(facNeto)}`);
            console.log(`    COGS contable:    $${fmt(cogs)}`);
            console.log(`    Margen bruto:     $${fmt(margenAbs)}  (${margenPct.toFixed(1)}%)`);
        }
        if (compras > 0) {
            const margenC = facNeto - compras;
            const margenPctC = (margenC / facNeto) * 100;
            console.log(`  Vía COMPRAS (referencial):`);
            console.log(`    Facturación neta: $${fmt(facNeto)}`);
            console.log(`    Compras netas:    $${fmt(compras)}`);
            console.log(`    Diferencia:       $${fmt(margenC)}  (${margenPctC.toFixed(1)}%)`);
        }

        out.months[ym] = { facturacion: A, compras: C, cogsContable: B };
    }

    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const file = path.resolve(process.cwd(), `tmp/margin_final_${stamp}.json`);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify(out, null, 2));
    console.log(`\nDump completo: ${file}`);
}

main().catch(e => { console.error('FATAL', e); process.exit(1); });
