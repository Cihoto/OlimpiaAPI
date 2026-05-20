// GET-only. Margen acumulado YTD ene–may 2026, desglosado:
//  - Facturación neta del período (facturas - notas crédito)
//  - Compras del período, separadas en:
//      · Mercadería importada (PROVEEDOR EXTRANJERO 55555555-5 + logística internación)
//      · Mercadería local (proveedores reales no-factoring, no-retailers)
//      · Cargos de canal/retailers (notas débito de Cencosud/Walmart/Tottus/Oxxo)
//      · Factoring (operación financiera, se excluye del margen operacional)
//      · Notas crédito de proveedores (se restan)
//  - Margen estimado y vista mes a mes

import 'dotenv/config';
import fs from 'fs';
import path from 'path';

const BASE = 'https://api.defontana.com/api/';

const months = ['2026-01', '2026-02', '2026-03', '2026-04', '2026-05'];

const FACTORING_RUTS = new Set([
    '77078244-9', '76562786-9', '96655860-1', '76197101-8',
    '97004000-5', '81537600-5', '78627210-6'
]);
// Retailers que típicamente le COBRAN a Olimpia vía NC/ND (no son proveedores de mercadería)
const RETAILER_RUTS = new Set([
    '81201000-K', '76042014-K', '76084682-1', '78627210-6'
]);
const FOREIGN_RUTS = new Set(['55555555-5']);
const LOGISTICS_HINTS = /LOGISTICA|MERCOSUR|MEGA FRIO|TRANSPORT/i;

function monthRange(yyyymm) {
    const [y, m] = yyyymm.split('-').map(Number);
    const last = new Date(y, m, 0).getDate();
    return { y, ini: `${y}-${String(m).padStart(2,'0')}-01`, fin: `${y}-${String(m).padStart(2,'0')}-${String(last).padStart(2,'0')}` };
}

async function auth() {
    const url = `${BASE}Auth?client=${process.env.ID_CLIENTE}&company=${process.env.ID_EMPRESA}&user=${process.env.ID_USUARIO}&password=${process.env.PASSWORD}`;
    const r = await fetch(url, { method: 'GET', headers: { 'accept': 'text/plain' } });
    const j = await r.json();
    return j.access_token;
}
async function G(p, k) {
    const r = await fetch(BASE + p, { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${k}` } });
    const t = await r.text();
    try { return { s: r.status, b: JSON.parse(t) }; } catch { return { s: r.status, b: t }; }
}
const parseCLP = s => typeof s === 'number' ? s : Number(String(s || 0).replace(/[^\d-]/g, '')) || 0;
const fmt = n => Number.isFinite(n) ? Math.round(n).toLocaleString('es-CL') : String(n);

async function ventasMes(apiKey, ini, fin) {
    const all = [];
    for (let p = 1; p <= 200; p++) {
        const { b } = await G(`Sale/GetSalebyDate?initialDate=${ini}&endingDate=${fin}&status=0&itemsPerPage=100&pageNumber=${p}`, apiKey);
        if (!b?.success) break;
        const list = b.saleList || [];
        all.push(...list);
        if (!list.length || all.length >= (b.totalItems || 0)) break;
        await new Promise(r => setTimeout(r, 60));
    }
    const FACT = ['FVAELECT', 'FENAELECT', 'BELECT', 'BENELECT', 'FVAEXP', 'FCEM', 'FACELECT'];
    const NC   = ['NCVELECT', 'NCEELECT', 'NCREELECT', 'NCELECT', 'NCRELECT'];
    let fNeto = 0, fTotal = 0, fCount = 0;
    let cNeto = 0, cTotal = 0, cCount = 0;
    for (const s of all) {
        const t = s.documentType;
        if (FACT.includes(t)) { fNeto += s.affectableTotal || 0; fTotal += s.total || 0; fCount++; }
        if (NC.includes(t))   { cNeto += s.affectableTotal || 0; cTotal += s.total || 0; cCount++; }
    }
    return { fCount, cCount, fNeto, fTotal, cNeto, cTotal, netoFacturado: fNeto - cNeto, totalConIva: fTotal - cTotal };
}

async function comprasMes(apiKey, ini, fin) {
    const all = [];
    let total = null;
    for (let page = 0; page < 500; page++) {
        const { b } = await G(`Purchase/List?StartDate=${ini}&FinishDate=${fin}&ItemsPerPage=100&Page=${page}`, apiKey);
        if (!b?.success) break;
        const data = b.data || {};
        if (page === 0) total = data.recordsTotal ?? null;
        const list = data.data || [];
        all.push(...list);
        if (!list.length) break;
        if (total && all.length >= total) break;
        if (list.length < 100) break;
        await new Promise(r => setTimeout(r, 60));
    }
    // Clasificación
    const buckets = {
        importada:  { count: 0, brutoConIva: 0, neto: 0 },
        local:      { count: 0, brutoConIva: 0, neto: 0 },
        retailer:   { count: 0, brutoConIva: 0, neto: 0 },
        factoring:  { count: 0, brutoConIva: 0, neto: 0 },
        ncProveedor:{ count: 0, brutoConIva: 0, neto: 0 },
        logistica:  { count: 0, brutoConIva: 0, neto: 0 }
    };
    const byProv = {};
    for (const d of all) {
        const sii = String(d.siiDocumentType || '');
        const tot = parseCLP(d.documentTotal);
        const isExento = ['34', '41', '110', '111', '112'].includes(sii);
        const isNC = ['61', '60', '112'].includes(sii);
        const neto = isExento ? tot : Math.round(tot / 1.19);
        const rut = d.providerLegalCode || '?';
        const name = d.providerName || '';
        byProv[rut] = byProv[rut] || { name, count: 0, brutoConIva: 0, neto: 0 };
        byProv[rut].count++;
        const signedTot = isNC ? -tot : tot;
        const signedNeto = isNC ? -neto : neto;
        byProv[rut].brutoConIva += signedTot;
        byProv[rut].neto += signedNeto;
        let bucket = null;
        if (FACTORING_RUTS.has(rut)) bucket = 'factoring';
        else if (isNC) bucket = 'ncProveedor';
        else if (FOREIGN_RUTS.has(rut)) bucket = 'importada';
        else if (RETAILER_RUTS.has(rut)) bucket = 'retailer';
        else if (LOGISTICS_HINTS.test(name)) bucket = 'logistica';
        else bucket = 'local';
        buckets[bucket].count++;
        buckets[bucket].brutoConIva += signedTot;
        buckets[bucket].neto += signedNeto;
    }
    return { totalReportado: total, docs: all.length, buckets, byProv };
}

function pct(n, d) { return d ? (n / d * 100).toFixed(1) + '%' : '—'; }

async function main() {
    const apiKey = await auth();
    console.log('Auth OK\n');
    const out = { generatedAt: new Date().toISOString(), period: 'ene–may 2026', months: {} };

    const acumF = { fNeto: 0, cNeto: 0, fTotal: 0, cTotal: 0, fCount: 0, cCount: 0 };
    const acumC = {
        importada: { count: 0, brutoConIva: 0, neto: 0 },
        local:     { count: 0, brutoConIva: 0, neto: 0 },
        retailer:  { count: 0, brutoConIva: 0, neto: 0 },
        factoring: { count: 0, brutoConIva: 0, neto: 0 },
        ncProveedor:{ count: 0, brutoConIva: 0, neto: 0 },
        logistica: { count: 0, brutoConIva: 0, neto: 0 }
    };
    const acumByProv = {};

    console.log('┌─────────┬──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐');
    console.log('│  Mes    │ Facturación  │ Compras totl │ Importada    │ Local        │ Retailers ND │');
    console.log('├─────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────┤');

    for (const ym of months) {
        const { ini, fin } = monthRange(ym);
        process.stderr.write(`\r  procesando ${ym}...           `);
        const V = await ventasMes(apiKey, ini, fin);
        const C = await comprasMes(apiKey, ini, fin);

        acumF.fNeto += V.fNeto; acumF.cNeto += V.cNeto; acumF.fTotal += V.fTotal; acumF.cTotal += V.cTotal;
        acumF.fCount += V.fCount; acumF.cCount += V.cCount;
        for (const k of Object.keys(acumC)) {
            acumC[k].count += C.buckets[k].count;
            acumC[k].brutoConIva += C.buckets[k].brutoConIva;
            acumC[k].neto += C.buckets[k].neto;
        }
        for (const [rut, v] of Object.entries(C.byProv)) {
            acumByProv[rut] = acumByProv[rut] || { name: v.name, count: 0, neto: 0 };
            acumByProv[rut].count += v.count;
            acumByProv[rut].neto += v.neto;
        }

        const lineFac = fmt(V.netoFacturado).padStart(13);
        const lineTot = fmt(C.buckets.importada.neto + C.buckets.local.neto + C.buckets.retailer.neto + C.buckets.logistica.neto).padStart(13);
        const lineImp = fmt(C.buckets.importada.neto).padStart(13);
        const lineLoc = fmt(C.buckets.local.neto).padStart(13);
        const lineRet = fmt(C.buckets.retailer.neto).padStart(13);
        console.log(`│ ${ym} │ ${lineFac}│ ${lineTot}│ ${lineImp}│ ${lineLoc}│ ${lineRet}│`);

        out.months[ym] = { ventas: V, compras: C };
    }
    process.stderr.write('\r                          \r');
    console.log('└─────────┴──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘\n');

    const totalFac = acumF.fNeto - acumF.cNeto;
    const costoMerc = acumC.importada.neto + acumC.local.neto + acumC.logistica.neto;
    const costoCanal = acumC.retailer.neto;
    const costoOperTotal = costoMerc + costoCanal;
    const margenSoloMerc = totalFac - costoMerc;
    const margenOperacional = totalFac - costoOperTotal;

    console.log('═══════════════════════════════════════════════════════════════════════');
    console.log('                    ACUMULADO YTD ENE–MAY 2026');
    console.log('═══════════════════════════════════════════════════════════════════════');
    console.log('');
    console.log('  FACTURACIÓN:');
    console.log(`    Facturas:         ${String(acumF.fCount).padStart(4)} docs  neto $${fmt(acumF.fNeto)}`);
    console.log(`    Notas crédito:    ${String(acumF.cCount).padStart(4)} docs  neto $${fmt(acumF.cNeto)}`);
    console.log(`    ▶ NETO FACTURADO:                      $${fmt(totalFac)}`);
    console.log('');
    console.log('  COSTOS (compras netas, excluyendo IVA, todo período):');
    for (const [k, v] of Object.entries(acumC)) {
        console.log(`    ${k.padEnd(12)} ${String(v.count).padStart(4)} docs  neto $${fmt(v.neto).padStart(15)}`);
    }
    console.log('');
    console.log('  ────────── COSTO DE MERCADERÍA (importada + local + logística) ──────────');
    console.log(`    $${fmt(costoMerc)}   (${pct(costoMerc, totalFac)} de la facturación)`);
    console.log('');
    console.log('  ────────── COSTOS DE CANAL (notas débito retailers) ──────────');
    console.log(`    $${fmt(costoCanal)}   (${pct(costoCanal, totalFac)} de la facturación)`);
    console.log('');
    console.log('  ═══════════════ MARGEN OPERACIONAL ESTIMADO ═══════════════');
    console.log(`    Margen sólo restando mercadería:   $${fmt(margenSoloMerc)}   (${pct(margenSoloMerc, totalFac)})`);
    console.log(`    Margen restando merc + canal:      $${fmt(margenOperacional)}   (${pct(margenOperacional, totalFac)})`);
    console.log('');
    console.log('  ⚠ NOTAS IMPORTANTES:');
    console.log('  · El "margen" es ESTIMADO: compras ene–may ≠ COGS ene–may si hay desfase de inventario');
    console.log('    (lo importado en ene puede venderse en abr). Tomado como ventana de 5 meses se suaviza.');
    console.log('  · No incluye sueldos, arriendos, servicios ni gastos administrativos.');
    console.log('  · El COGS exacto requiere cargar el costo unitario por producto en Defontana.');
    console.log('  · El factoring no es costo operacional (es decisión financiera), se excluyó.');

    console.log('\n  Top 15 proveedores REALES de mercadería (ene–may, neto acumulado):');
    const top = Object.entries(acumByProv)
        .filter(([rut]) => !FACTORING_RUTS.has(rut))
        .sort((a,b) => b[1].neto - a[1].neto)
        .slice(0, 15);
    for (const [rut, v] of top) {
        console.log(`    ${rut.padEnd(15)} ${(v.name||'').slice(0,42).padEnd(42)} neto $${fmt(v.neto).padStart(13)} (${v.count} docs)`);
    }

    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const file = path.resolve(process.cwd(), `tmp/margin_ytd_${stamp}.json`);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify(out, null, 2));
    console.log(`\nDump completo: ${file}`);
}

main().catch(e => { console.error('FATAL', e); process.exit(1); });
