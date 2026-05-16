// GET-only. Escanea los últimos N folios de un documentType y clasifica cada uno
// como sano / muerto / anulado / no_existe usando 3 endpoints por folio.
//
// Uso: node src/tools/scanLastFolios.js [count] [documentType]
//   count: cantidad de folios hacia atrás desde el último usado (default 100)
//   documentType: FVAELECT por default

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fetchApiKey, getApiKey } from '../middleware/auth.js';

const COUNT = Number(process.argv[2] || 100);
const DOC_TYPE = process.argv[3] || 'FVAELECT';
const SALE_BASE = (process.env.SALE_API_URL || 'https://api.defontana.com/api/Sale/').replace(/\/+$/, '/');

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function GET(url, apiKey) {
    const r = await fetch(url, { method: 'GET', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` } });
    const text = await r.text();
    let body;
    try { body = JSON.parse(text); } catch { body = text; }
    return { status: r.status, body };
}

function classify({ sale, xml, sii }) {
    // sale.body puede venir en forma { "0": {...} } o vacío {}
    const saleData = sale?.body?.['0'] || sale?.body?.sale || null;
    const xmlOk = xml?.body?.success === true && xml?.body?.document;
    const xmlFail = xml?.body?.success === false;
    const siiStates = sii?.body?.states || [];
    const lastSiiCode = siiStates.length ? siiStates[siiStates.length - 1].code : null;
    const dfStatus = saleData?.status || null;

    let cls;
    if (!saleData) {
        cls = 'no_existe';
    } else if (String(dfStatus).toUpperCase() === 'ANULADO') {
        cls = 'anulado';
    } else if (xmlOk && lastSiiCode === 'Emitido') {
        cls = 'sano';
    } else if (!xmlOk && (siiStates.length === 0)) {
        cls = 'muerto';
    } else if (xmlOk && !lastSiiCode) {
        cls = 'sin_estado_sii';
    } else if (xmlFail) {
        cls = 'muerto';
    } else {
        cls = 'otro';
    }

    return {
        clasificacion: cls,
        defontanaStatus: dfStatus,
        xmlSuccess: xml?.body?.success ?? null,
        xmlMessage: xml?.body?.message ?? null,
        siiCount: siiStates.length,
        siiLastCode: lastSiiCode,
        siiHistory: siiStates,
        emissionDate: saleData?.emissionDate || null,
        dateTime: saleData?.dateTime || null,
        clientFile: saleData?.clientFile || null,
        sellerFileId: saleData?.sellerFileId || null,
        issuingUser: saleData?.issuingUser || null,
        total: saleData?.total || null,
        externalId: saleData?.externalDocumentID || null,
        gloss: saleData?.gloss || null
    };
}

async function main() {
    await fetchApiKey();
    const apiKey = getApiKey();

    // Conseguir último folio usado
    const last = await GET(`${SALE_BASE}GetLastUsedFolio?documentType=${encodeURIComponent(DOC_TYPE)}`, apiKey);
    const lastFolio = last?.body?.folio;
    if (!lastFolio) { console.error('No se pudo obtener GetLastUsedFolio:', last); process.exit(1); }

    const startFolio = lastFolio - COUNT + 1;
    console.log(`[scan] docType=${DOC_TYPE} | rango folios ${startFolio} -> ${lastFolio} (${COUNT} folios)`);

    const out = [];
    let i = 0;
    for (let folio = startFolio; folio <= lastFolio; folio++) {
        i++;
        const sale = await GET(`${SALE_BASE}GetSale?documentType=${encodeURIComponent(DOC_TYPE)}&number=${folio}`, apiKey);
        const xml  = await GET(`${SALE_BASE}GetXMLDocumentBase64?documentType=${encodeURIComponent(DOC_TYPE)}&number=${folio}`, apiKey);
        const sii  = await GET(`${SALE_BASE}GetAllSIIStates?documentType=${encodeURIComponent(DOC_TYPE)}&number=${folio}`, apiKey);

        const c = classify({ sale, xml, sii });
        out.push({ folio, ...c });

        if (i % 10 === 0) {
            const buckets = {};
            out.forEach(o => { buckets[o.clasificacion] = (buckets[o.clasificacion]||0)+1; });
            console.log(`[scan] ${i}/${COUNT} folio=${folio} -> ${c.clasificacion}   parcial:`, buckets);
        }
        await sleep(80);
    }

    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const file = path.resolve(process.cwd(), `scan_last_${COUNT}_${DOC_TYPE}_${stamp}.json`);
    fs.writeFileSync(file, JSON.stringify(out, null, 2));

    // Resumen final
    const buckets = {};
    out.forEach(o => { buckets[o.clasificacion] = (buckets[o.clasificacion]||0)+1; });
    console.log('\n=== RESUMEN FINAL ===');
    console.log(buckets);
    console.log(`Dump: ${file}`);

    const muertos = out.filter(o => o.clasificacion === 'muerto');
    const anulados = out.filter(o => o.clasificacion === 'anulado');
    const otros = out.filter(o => !['sano','muerto','anulado','no_existe'].includes(o.clasificacion));

    if (muertos.length) {
        console.log(`\n🚨 FOLIOS MUERTOS (${muertos.length}):`);
        for (const m of muertos) {
            console.log(`  folio=${m.folio} dt=${m.dateTime} emision=${m.emissionDate} cliente=${m.clientFile} total=${m.total} status=${m.defontanaStatus} xmlMsg="${m.xmlMessage}" siiCount=${m.siiCount}`);
        }
    }
    if (anulados.length) {
        console.log(`\nAnulados (${anulados.length}):`);
        for (const m of anulados) console.log(`  folio=${m.folio} cliente=${m.clientFile} total=${m.total}`);
    }
    if (otros.length) {
        console.log(`\nOtros (${otros.length}):`);
        for (const m of otros) console.log(`  folio=${m.folio} -> ${m.clasificacion} dfStatus=${m.defontanaStatus} sii=${m.siiLastCode}`);
    }
}

main().catch(e => { console.error(e); process.exit(1); });
