// GET/POST de lectura. Busca el COSTO operacional desde 3 ángulos:
//  1) Purchase/List → inspecciona la respuesta cruda
//  2) Accounting/GetVoucherList → comprobantes de centralización (LV/CV...)
//     Luego abre uno con GetVoucher para ver si trae las cuentas de costo
//  3) Inventory/List (POST) → movimientos de inventario con costo unitario

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
    return { ini: `${y}-${String(m).padStart(2,'0')}-01`, fin: `${y}-${String(m).padStart(2,'0')}-${String(last).padStart(2,'0')}` };
}

async function auth() {
    const url = `${BASE}Auth?client=${process.env.ID_CLIENTE}&company=${process.env.ID_EMPRESA}&user=${process.env.ID_USUARIO}&password=${process.env.PASSWORD}`;
    const r = await fetch(url, { method: 'GET', headers: { 'accept': 'text/plain' } });
    const j = await r.json();
    return j.access_token;
}

async function http(method, pathAndQuery, apiKey, body) {
    const r = await fetch(BASE + pathAndQuery, {
        method,
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
        body: body ? JSON.stringify(body) : undefined
    });
    const text = await r.text();
    try { return { status: r.status, body: JSON.parse(text) }; } catch { return { status: r.status, body: text }; }
}
const G = (p, k) => http('GET', p, k);
const P = (p, k, b) => http('POST', p, k, b);

const fmt = n => Number.isFinite(n) ? Math.round(n).toLocaleString('es-CL') : String(n);

async function main() {
    const apiKey = await auth();
    console.log('Auth OK\n');
    const out = { months: {} };

    // ====== TipoVouchers globales ======
    console.log('--- GetTypeVoucherInfo ---');
    const tvi = await G('Accounting/GetTypeVoucherInfo', apiKey);
    console.log(`HTTP ${tvi.status} success=${tvi.body?.success} keys=${Object.keys(tvi.body || {}).slice(0,8)}`);
    let voucherTypes = [];
    for (const k of Object.keys(tvi.body || {})) {
        if (Array.isArray(tvi.body[k])) { voucherTypes = tvi.body[k]; break; }
    }
    console.log(`Tipos de comprobante: ${voucherTypes.length}`);
    voucherTypes.slice(0, 30).forEach(v => console.log('  ', v));

    // ====== Tipos de inventario ======
    console.log('\n--- Inventory/GetTypeInventoryInfo ---');
    const inv = await G('Inventory/GetTypeInventoryInfo', apiKey);
    console.log(`HTTP ${inv.status} success=${inv.body?.success}`);
    let invTypes = [];
    for (const k of Object.keys(inv.body || {})) {
        if (Array.isArray(inv.body[k])) { invTypes = inv.body[k]; break; }
    }
    console.log(`Tipos de inventario: ${invTypes.length}`);
    invTypes.slice(0, 30).forEach(v => console.log('  ', v));

    for (const ym of months) {
        const { ini, fin } = monthRange(ym);
        console.log(`\n========== ${ym} (${ini} → ${fin}) ==========`);
        const mo = {};

        // ----- 1) Purchase/List crudo -----
        console.log('\n[1] Purchase/List - raw response page 0');
        const pl = await G(`Purchase/List?StartDate=${ini}&FinishDate=${fin}&ItemsPerPage=50&Page=0`, apiKey);
        console.log(`HTTP ${pl.status}  keys=${Object.keys(pl.body || {}).join(',')}`);
        console.log('Raw body (first 1500 chars):');
        console.log(JSON.stringify(pl.body, null, 2).slice(0, 1500));
        mo.purchaseListRaw = pl.body;

        // ----- 2) GetVoucherList para el período -----
        console.log('\n[2] Accounting/GetVoucherList - todos los comprobantes del mes');
        const vl = await G(`Accounting/GetVoucherList?FromDate=${ini}&ToDate=${fin}&ItemsPerPage=50&Page=0`, apiKey);
        console.log(`HTTP ${vl.status} success=${vl.body?.success} msg=${vl.body?.message || ''}`);
        const vlKeys = Object.keys(vl.body || {});
        console.log('  keys:', vlKeys);
        if (vl.body?.success) {
            // Encontrar la lista
            let vlist = null;
            for (const k of ['vouchers', 'voucherList', 'list', 'data', 'items']) {
                if (Array.isArray(vl.body[k])) { vlist = vl.body[k]; break; }
            }
            if (!vlist) for (const v of Object.values(vl.body)) if (Array.isArray(v) && typeof v[0] === 'object') { vlist = v; break; }
            console.log(`  Voucher list size (1ra página): ${vlist?.length || 0}`);
            if (vlist?.[0]) {
                console.log('  keys voucher[0]:', Object.keys(vlist[0]));
                console.log('  sample voucher[0]:', JSON.stringify(vlist[0], null, 2).slice(0, 800));
                mo.voucherSample = vlist[0];

                // Contar tipos
                const byType = {};
                for (const v of vlist) {
                    const t = v.voucherType || v.type || v.voucherTypeId || '?';
                    byType[t] = (byType[t] || 0) + 1;
                }
                console.log('  Por tipo (1ra página):', byType);
                mo.voucherByTypePage0 = byType;

                // Abrir el primer voucher con GetVoucher para ver las cuentas
                const sample = vlist[0];
                const vt = sample.voucherType || sample.type || sample.voucherTypeId;
                const num = sample.voucherNumber || sample.number || sample.voucherNum;
                const fy = sample.fiscalYear || new Date().getFullYear();
                if (vt && num) {
                    console.log(`\n  Abriendo GetVoucher(${vt}, num=${num}, year=${fy})...`);
                    const gv = await G(`Accounting/GetVoucher?VoucherType=${encodeURIComponent(vt)}&FiscalYear=${fy}&Number=${num}`, apiKey);
                    console.log(`  HTTP ${gv.status} success=${gv.body?.success} msg=${gv.body?.message || ''}`);
                    if (gv.body?.success) {
                        const v = gv.body;
                        console.log('  voucher top keys:', Object.keys(v).slice(0, 20));
                        // Buscar las líneas
                        let lines = null;
                        for (const k of ['movements', 'lines', 'details', 'movs', 'data']) {
                            if (Array.isArray(v[k])) { lines = v[k]; break; }
                        }
                        if (!lines) for (const val of Object.values(v)) if (Array.isArray(val) && typeof val[0] === 'object') { lines = val; break; }
                        if (lines?.[0]) {
                            console.log(`  voucher tiene ${lines.length} movimientos`);
                            console.log('  keys mov[0]:', Object.keys(lines[0]));
                            console.log('  mov[0]:', JSON.stringify(lines[0]));
                            mo.voucherDetailSample = { keys: Object.keys(lines[0]), sample: lines[0] };
                        }
                    }
                }
            }
        }

        // ----- 3) Inventory/List (POST, payload mínimo) -----
        console.log('\n[3] Inventory/List - movimientos de inventario del mes');
        const invBody = { startDate: ini, finishDate: fin, itemsPerPage: 50, page: 0 };
        const il = await P('Inventory/List', apiKey, invBody);
        console.log(`HTTP ${il.status} success=${il.body?.success} msg=${il.body?.message || ''}`);
        const ilKeys = Object.keys(il.body || {});
        console.log('  keys:', ilKeys);
        if (il.body?.success === undefined && il.body?.exceptionMessage) {
            console.log('  exception:', il.body.exceptionMessage);
        }
        // Si falla, intento otra forma de payload
        if (!il.body?.success) {
            console.log('  → reintento con StartDate/FinishDate y campos en PascalCase');
            const alt = await P('Inventory/List', apiKey, { StartDate: ini, FinishDate: fin, ItemsPerPage: 50, Page: 0 });
            console.log(`  HTTP ${alt.status} success=${alt.body?.success} msg=${alt.body?.message || ''}`);
            console.log('  body preview:', JSON.stringify(alt.body, null, 2).slice(0, 800));
            mo.inventoryListRaw = alt.body;
        } else {
            console.log('  body preview:', JSON.stringify(il.body, null, 2).slice(0, 800));
            mo.inventoryListRaw = il.body;
        }

        // ----- 4) GetDocumentAssociatedVouchers para una factura del mes -----
        // (esto nos diría si la factura tiene un voucher contable con el COGS)
        console.log('\n[4] DocumentAssociatedVouchers para 1 factura del mes');
        // Tomar 1 folio FVAELECT del mes - mejor lo paso por arg pero sample-ar es OK
        const sales = await G(`Sale/GetSalebyDate?initialDate=${ini}&endingDate=${fin}&status=0&itemsPerPage=10&pageNumber=1&documentType=FVAELECT`, apiKey);
        const fvae = (sales.body?.saleList || []).find(s => s.documentType === 'FVAELECT');
        if (fvae) {
            const folio = fvae.firstFolio;
            const dav = await G(`Accounting/GetDocumentAssociatedVouchers?DocumentType=FVAELECT&Folio=${folio}`, apiKey);
            console.log(`  Factura ${folio}: HTTP ${dav.status} success=${dav.body?.success} msg=${dav.body?.message || ''}`);
            console.log('  body preview:', JSON.stringify(dav.body, null, 2).slice(0, 1200));
            mo.docAssociatedVouchersSample = { folio, body: dav.body };
        }

        out.months[ym] = mo;
    }

    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const file = path.resolve(process.cwd(), `tmp/cost_routes_${stamp}.json`);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify(out, null, 2));
    console.log(`\nDump: ${file}`);
}

main().catch(e => { console.error('FATAL', e); process.exit(1); });
