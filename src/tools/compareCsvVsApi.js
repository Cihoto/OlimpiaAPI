// Compara CSV bruto de Antonio (InfCobranzaDetallado.csv) con el reporte de la API.
// Aplica las mismas reglas de filtrado: solo morosos (Saldo > 0 && Vencimiento < hoy), excluye 11.111.111-1.

import fs from 'fs';
import path from 'path';
import { fetchApiKey, getApiKey } from '../middleware/auth.js';
import { generateMorosasReport } from '../services/morosasReportService.js';

const CSV_PATH = path.resolve('c:/OLIMPIA/FRONT_BACK/features/InfCobranzaDetallado.csv');

// Parser CSV con respeto a comillas
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

function parseNumber(s) {
    if (!s) return 0;
    const cleaned = String(s).replace(/[$,\s]/g, '').replace(/^-/, '-');
    const n = parseFloat(cleaned);
    return Number.isFinite(n) ? n : 0;
}

function parseDate(dmy) {
    // formato DD/MM/YYYY
    const m = /^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/.exec(dmy);
    if (!m) return null;
    const [, d, mo, y] = m;
    return new Date(Number(y), Number(mo) - 1, Number(d));
}

async function main() {
    const raw = fs.readFileSync(CSV_PATH, 'utf8');
    const rows = parseCsv(raw);
    const header = rows[0];
    const idx = Object.fromEntries(header.map((h, i) => [h, i]));
    console.log('Columnas CSV:', header);

    const today = new Date();
    const csvDocs = [];

    for (let i = 1; i < rows.length; i++) {
        const r = rows[i];
        if (r.length < 5) continue;
        const rut = (r[idx.Rut] || '').trim();
        if (rut === '11.111.111-1') continue; // siempre excluir
        const name = r[idx.Nombre] || '';
        const tipo = r[idx.TipoDocumento] || '';
        const folio = (r[idx.DocNumero] || '').trim();
        const fechaVenc = parseDate(r[idx.FechaVencimiento]);
        const saldo = parseNumber(r[idx.Saldo]);
        const vendedor = r[idx.NombreVendedor] || '';

        csvDocs.push({ rut, name, tipo, folio, fechaVenc, saldo, vendedor });
    }

    // Filtros para morosos: Saldo > 0 && FechaVencimiento <= hoy
    const morosos = csvDocs.filter(d => d.saldo > 0 && d.fechaVenc && d.fechaVenc <= today);
    const creditosVencidos = csvDocs.filter(d => d.saldo < 0 && d.fechaVenc && d.fechaVenc <= today);

    const totalMoroso = morosos.reduce((s, d) => s + d.saldo, 0);
    const totalCreditos = creditosVencidos.reduce((s, d) => s + Math.abs(d.saldo), 0);

    console.log('\n===== CSV ANTONIO =====');
    console.log(`Total docs (sin GIRO GENERICO):       ${csvDocs.length}`);
    console.log(`Docs MOROSOS (saldo>0 + vencido):     ${morosos.length}`);
    console.log(`Total moroso CSV:                     $${totalMoroso.toLocaleString('es-CL')}`);
    console.log(`Docs créditos a favor vencidos:       ${creditosVencidos.length}`);
    console.log(`Total créditos a favor vencidos CSV:  $${totalCreditos.toLocaleString('es-CL')}`);

    // Distribución por tipo de documento
    const byType = {};
    for (const d of morosos) {
        if (!byType[d.tipo]) byType[d.tipo] = { count: 0, total: 0 };
        byType[d.tipo].count += 1;
        byType[d.tipo].total += d.saldo;
    }
    console.log('\nMorosos por tipo:');
    for (const [t, v] of Object.entries(byType).sort((a, b) => b[1].total - a[1].total)) {
        console.log(`  ${t.padEnd(40)}  ${String(v.count).padStart(4)}  $${v.total.toLocaleString('es-CL')}`);
    }

    // Top 10 clientes morosos del CSV
    const byClientCsv = {};
    for (const d of morosos) {
        if (!byClientCsv[d.rut]) byClientCsv[d.rut] = { rut: d.rut, name: d.name, total: 0, count: 0 };
        byClientCsv[d.rut].total += d.saldo;
        byClientCsv[d.rut].count += 1;
    }
    const topCsv = Object.values(byClientCsv).sort((a, b) => b.total - a.total).slice(0, 15);

    // ===== AHORA LA API =====
    console.log('\n\n===== API REPORTE =====');
    await fetchApiKey();
    const apiKey = getApiKey();
    const report = await generateMorosasReport(apiKey);
    const morosasApi = report.rows.filter(r => r.esMoroso);
    const totalApi = morosasApi.reduce((s, r) => s + r.saldo, 0);
    console.log(`Docs MOROSOS API:        ${morosasApi.length}`);
    console.log(`Total moroso API:        $${totalApi.toLocaleString('es-CL')}`);
    console.log(`Total créditos vencidos: $${report.summary.creditosTotal.toLocaleString('es-CL')}`);

    // Comparación top clientes
    console.log('\n===== TOP 15 CLIENTES =====');
    console.log('CSV (Antonio):                                                              vs    API:');
    const apiByClient = {};
    for (const r of morosasApi) {
        apiByClient[r.rut] = apiByClient[r.rut] || { rut: r.rut, name: r.clientName, total: 0, count: 0 };
        apiByClient[r.rut].total += r.saldo;
        apiByClient[r.rut].count += 1;
    }
    for (const c of topCsv) {
        const a = apiByClient[c.rut];
        const apiStr = a ? `$${a.total.toLocaleString('es-CL').padStart(15)} (${a.count})` : '         N/A         ';
        console.log(`${c.rut.padEnd(15)} ${c.name.substring(0, 35).padEnd(35)} $${c.total.toLocaleString('es-CL').padStart(15)} (${c.count})   vs   ${apiStr}`);
    }

    // Discrepancia agregada
    console.log('\n===== RESUMEN =====');
    const diff = totalApi - totalMoroso;
    console.log(`Diferencia API - CSV:    $${diff.toLocaleString('es-CL')}`);
    console.log(`% sobre CSV:             ${(diff / totalMoroso * 100).toFixed(2)}%`);

    fs.writeFileSync('comparison_result.json', JSON.stringify({
        csv: { total: totalMoroso, count: morosos.length, top: topCsv, byType, creditosVencidos: { total: totalCreditos, count: creditosVencidos.length } },
        api: { total: totalApi, count: morosasApi.length, top: Object.values(apiByClient).sort((a,b) => b.total - a.total).slice(0, 30), creditosTotal: report.summary.creditosTotal }
    }, null, 2));
    console.log('\nDump: comparison_result.json');
}

main().catch(e => { console.error(e); process.exit(1); });
