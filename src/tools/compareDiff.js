// Diff fino: identifica diferencias por cliente entre CSV y API.
import fs from 'fs';
import path from 'path';
import { fetchApiKey, getApiKey } from '../middleware/auth.js';
import { generateMorosasReport } from '../services/morosasReportService.js';

function parseCsv(text) {
    const rows = [];
    let cur = '', row = [], inQ = false;
    for (let i = 0; i < text.length; i++) {
        const ch = text[i];
        if (ch === '"' && !inQ) inQ = true;
        else if (ch === '"' && inQ) {
            if (text[i+1] === '"') { cur += '"'; i++; }
            else inQ = false;
        }
        else if (ch === ',' && !inQ) { row.push(cur); cur = ''; }
        else if ((ch === '\n' || ch === '\r') && !inQ) {
            if (cur || row.length) { row.push(cur); rows.push(row); }
            cur = ''; row = [];
            if (ch === '\r' && text[i+1] === '\n') i++;
        }
        else cur += ch;
    }
    if (cur || row.length) { row.push(cur); rows.push(row); }
    return rows;
}
function parseNum(s) { return parseFloat(String(s||'').replace(/[$,\s]/g, '')) || 0; }
function parseDate(d) {
    const m = /^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/.exec(d);
    return m ? new Date(+m[3], +m[2]-1, +m[1]) : null;
}

async function main() {
    const text = fs.readFileSync('c:/OLIMPIA/FRONT_BACK/features/InfCobranzaDetallado.csv', 'utf8');
    const rows = parseCsv(text);
    const h = rows[0];
    const idx = Object.fromEntries(h.map((c,i)=>[c.replace(/^﻿/, ''), i]));
    const today = new Date();

    const byRutCsv = {};
    for (let i=1; i<rows.length; i++) {
        const r = rows[i];
        if (r.length < 5) continue;
        const rut = (r[idx.Rut]||'').trim();
        if (rut === '11.111.111-1') continue;
        const saldo = parseNum(r[idx.Saldo]);
        const venc = parseDate(r[idx.FechaVencimiento]);
        if (!(saldo > 0 && venc && venc <= today)) continue;
        byRutCsv[rut] = byRutCsv[rut] || { rut, name: r[idx.Nombre], total: 0, count: 0, folios: [] };
        byRutCsv[rut].total += saldo;
        byRutCsv[rut].count++;
        byRutCsv[rut].folios.push({ folio: r[idx.DocNumero], saldo, venc: r[idx.FechaVencimiento] });
    }

    await fetchApiKey();
    const apiKey = getApiKey();
    const report = await generateMorosasReport(apiKey);
    const morosas = report.rows.filter(r => r.esMoroso);

    const byRutApi = {};
    for (const r of morosas) {
        byRutApi[r.rut] = byRutApi[r.rut] || { rut: r.rut, name: r.clientName, total: 0, count: 0, folios: [] };
        byRutApi[r.rut].total += r.saldo;
        byRutApi[r.rut].count++;
        byRutApi[r.rut].folios.push({ folio: r.folio, saldo: r.saldo, venc: r.fechaVencimiento.substring(0,10) });
    }

    // Calcular diferencias
    const allRuts = new Set([...Object.keys(byRutCsv), ...Object.keys(byRutApi)]);
    const diffs = [];
    for (const rut of allRuts) {
        const csv = byRutCsv[rut];
        const api = byRutApi[rut];
        const cTotal = csv?.total || 0;
        const aTotal = api?.total || 0;
        const diff = aTotal - cTotal;
        if (Math.abs(diff) > 1) {
            diffs.push({ rut, name: csv?.name || api?.name, csvTotal: cTotal, apiTotal: aTotal, diff, csvCount: csv?.count||0, apiCount: api?.count||0 });
        }
    }
    diffs.sort((a,b) => Math.abs(b.diff) - Math.abs(a.diff));

    console.log('===== CLIENTES CON DISCREPANCIA =====');
    let totalDiff = 0;
    for (const d of diffs.slice(0, 30)) {
        console.log(`${d.rut.padEnd(15)} ${(d.name||'').substring(0,35).padEnd(35)} CSV=$${d.csvTotal.toLocaleString('es-CL').padStart(12)}(${d.csvCount})  API=$${d.apiTotal.toLocaleString('es-CL').padStart(12)}(${d.apiCount})  diff=$${d.diff.toLocaleString('es-CL').padStart(12)}`);
        totalDiff += d.diff;
    }
    console.log(`\nSuma de diferencias: $${totalDiff.toLocaleString('es-CL')}`);
    console.log(`Diferencias visibles: ${diffs.length}`);

    // Detalle de top 3 con mayor diff
    console.log('\n===== DETALLE FOLIOS - TOP 3 DIFF =====');
    for (const d of diffs.slice(0, 3)) {
        console.log(`\n--- ${d.rut} ${d.name} ---`);
        const csvFolios = new Map((byRutCsv[d.rut]?.folios || []).map(f => [String(f.folio), f]));
        const apiFolios = new Map((byRutApi[d.rut]?.folios || []).map(f => [String(f.folio), f]));
        const allFolios = new Set([...csvFolios.keys(), ...apiFolios.keys()]);
        for (const fol of allFolios) {
            const c = csvFolios.get(fol);
            const a = apiFolios.get(fol);
            const cs = c?.saldo || 0;
            const as = a?.saldo || 0;
            const flag = Math.abs(cs - as) > 1 ? ' ⚠' : '';
            console.log(`  folio ${fol.padEnd(10)} CSV=$${cs.toLocaleString('es-CL').padStart(12)} venc=${c?.venc||'-'.padEnd(10)}  |  API=$${as.toLocaleString('es-CL').padStart(12)} venc=${a?.venc||'-'}${flag}`);
        }
    }
}
main().catch(e => { console.error(e); process.exit(1); });
