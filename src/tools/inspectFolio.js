// GET-only. Llama a todos los endpoints de inspección de Defontana para UN folio.
// Uso: node src/tools/inspectFolio.js <folio> [documentType]

import 'dotenv/config';
import { fetchApiKey, getApiKey } from '../middleware/auth.js';
import fs from 'fs';
import path from 'path';

const folio = Number(process.argv[2]);
const documentType = process.argv[3] || 'FVAELECT';
if (!folio) { console.error('Uso: node inspectFolio.js <folio> [documentType]'); process.exit(1); }

const SALE_BASE = (process.env.SALE_API_URL || 'https://api.defontana.com/api/Sale/').replace(/\/+$/, '/');

async function GET(url, apiKey) {
    const t0 = Date.now();
    const r = await fetch(url, { method: 'GET', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` } });
    const text = await r.text();
    let body;
    try { body = JSON.parse(text); } catch { body = text; }
    return { status: r.status, ms: Date.now() - t0, body };
}

function trimBase64(obj) {
    if (!obj || typeof obj !== 'object') return obj;
    const copy = {};
    for (const [k, v] of Object.entries(obj)) {
        if (typeof v === 'string' && v.length > 200 && /^[A-Za-z0-9+/=]+$/.test(v.slice(0, 100))) {
            copy[k] = `<base64 length=${v.length}>`;
        } else {
            copy[k] = v;
        }
    }
    return copy;
}

async function main() {
    console.log(`[inspect] folio=${folio} docType=${documentType}`);
    await fetchApiKey();
    const apiKey = getApiKey();

    const calls = [
        { name: 'GetSale', url: `${SALE_BASE}GetSale?documentType=${encodeURIComponent(documentType)}&number=${folio}` },
        { name: 'GetSIIState', url: `${SALE_BASE}GetSIIState?documentType=${encodeURIComponent(documentType)}&number=${folio}` },
        { name: 'GetAllSIIStates', url: `${SALE_BASE}GetAllSIIStates?documentType=${encodeURIComponent(documentType)}&number=${folio}` },
        { name: 'GetTed', url: `${SALE_BASE}GetTed?documentType=${encodeURIComponent(documentType)}&folio=${folio}` },
        { name: 'GetAssociatedDocumentsBySale', url: `${SALE_BASE}GetAssociatedDocumentsBySale?documentType=${encodeURIComponent(documentType)}&number=${folio}` },
        { name: 'GetXMLDocumentBase64', url: `${SALE_BASE}GetXMLDocumentBase64?documentType=${encodeURIComponent(documentType)}&number=${folio}` },
        { name: 'GetSaleStandardPDFDocumentBase64', url: `${SALE_BASE}GetSaleStandardPDFDocumentBase64?folio=${folio}` },
        { name: 'GetLastUsedFolio', url: `${SALE_BASE}GetLastUsedFolio?documentType=${encodeURIComponent(documentType)}` },
        { name: 'GetCafSummary (33,61,etc)', url: `${SALE_BASE}GetCafSummary?documentIds=33,34,39,41,43,56,61` }
    ];

    const out = {};
    for (const c of calls) {
        try {
            const res = await GET(c.url, apiKey);
            out[c.name] = { httpStatus: res.status, ms: res.ms, body: trimBase64(res.body) };
            const summary = res.body && typeof res.body === 'object'
                ? JSON.stringify({ success: res.body.success, message: res.body.message, state: res.body.state, count: Array.isArray(res.body.states) ? res.body.states.length : undefined }).substring(0, 200)
                : String(res.body).substring(0, 200);
            console.log(`[${c.name}] HTTP ${res.status} ${res.ms}ms -> ${summary}`);
        } catch (err) {
            out[c.name] = { error: err.message };
            console.log(`[${c.name}] ERROR ${err.message}`);
        }
    }

    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const file = path.resolve(process.cwd(), `inspect_folio_${folio}_${stamp}.json`);
    fs.writeFileSync(file, JSON.stringify(out, null, 2));
    console.log(`\nDump completo: ${file}`);
}

main().catch(e => { console.error(e); process.exit(1); });
