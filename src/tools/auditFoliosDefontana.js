// GET-only. Cruza Mongo (invoice_notifications + dispatch_records) contra Defontana
// para detectar facturas registradas en la API que NO quedaron aprobadas en SII.
//
// Uso:
//   node src/tools/auditFoliosDefontana.js [--limit N] [--since YYYY-MM-DD] [--docType FVAELECT]
//
// Salida: audit_orphans_<timestamp>.json + log resumen por consola.

import 'dotenv/config';
import { MongoClient } from 'mongodb';
import fs from 'fs';
import path from 'path';
import { fetchApiKey, getApiKey } from '../middleware/auth.js';

const args = process.argv.slice(2);
function getArg(name, fallback = null) {
    const idx = args.indexOf(name);
    if (idx === -1) return fallback;
    return args[idx + 1];
}

const LIMIT = Number(getArg('--limit', '0')) || 0; // 0 = todos
const SINCE = getArg('--since', null);             // YYYY-MM-DD opcional
const DOC_TYPE = getArg('--docType', 'FVAELECT');

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'Olimpia';
const INVOICE_COL = process.env.MONGO_INVOICE_NOTIFICATION_COLLECTION || 'invoice_notifications';
const DISPATCH_COL = process.env.MONGO_DISPATCH_RECORDS_COLLECTION || 'dispatch_records';

const SALE_BASE = (process.env.SALE_API_URL || 'https://api.defontana.com/api/Sale/').replace(/\/+$/, '/') ;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function getJSON(url, apiKey) {
    const r = await fetch(url, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` }
    });
    return r.json();
}

function classifySIICode(code) {
    if (!code) return 'unknown';
    const c = String(code).toUpperCase().trim();
    // Valores observados de Defontana: "Emitido" = aceptado SII
    if (['EMITIDO', 'DOK', 'ACEPTADO', 'OK', 'ACE', 'EAC'].includes(c)) return 'aceptado';
    if (['RCH', 'RECHAZADO', 'RFR', 'DNK', 'FAU'].includes(c)) return 'rechazado';
    if (['RSC', 'RCT', 'REPARO', 'ACR', 'ACEPTADO CON REPARO'].includes(c)) return 'reparo';
    if (['EPR', 'SOK', 'PENDIENTE', 'NOK', 'EN PROCESO'].includes(c)) return 'pendiente';
    if (['ANULADO'].includes(c)) return 'anulado';
    return 'otro:' + c;
}

function classifyDefontanaStatus(status) {
    if (!status) return 'unknown';
    const s = String(status).toUpperCase().trim();
    if (s === 'CENTRALIZADO') return 'ok';
    if (s === 'ANULADO') return 'anulado';
    if (s.includes('NO CENTRALIZADO') || s.includes('NO_CENTRALIZADO')) return 'no_centralizado';
    if (s.includes('PENDIENTE')) return 'pendiente';
    if (s.includes('RECHAZ')) return 'rechazado';
    return 'otro:' + status;
}

async function getInvoiceIdsFromMongo() {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    const db = client.db(DB_NAME);

    const query = {};
    if (SINCE) {
        const isoStart = new Date(SINCE).toISOString();
        query.$or = [
            { sentAt: { $gte: new Date(isoStart) } },
            { createdAt: { $gte: new Date(isoStart) } }
        ];
    }

    const fromInvoices = await db.collection(INVOICE_COL)
        .find(query, { projection: { invoiceId: 1, rutCliente: 1, sentAt: 1, status: 1 } })
        .toArray();

    const fromDispatch = await db.collection(DISPATCH_COL)
        .find(query, { projection: { invoiceId: 1, clientRut: 1, clientName: 1, branchName: 1, dispatchDate: 1, externalDocumentID: 1, createdAt: 1 } })
        .toArray();

    await client.close();

    const map = new Map();
    for (const r of fromInvoices) {
        if (!r.invoiceId) continue;
        map.set(String(r.invoiceId), {
            invoiceId: String(r.invoiceId),
            rut: r.rutCliente || null,
            mongoSentAt: r.sentAt || null,
            mongoStatus: r.status || null,
            source: ['invoice_notifications']
        });
    }
    for (const r of fromDispatch) {
        if (!r.invoiceId) continue;
        const id = String(r.invoiceId);
        const existing = map.get(id) || { invoiceId: id, source: [] };
        existing.source.push('dispatch_records');
        existing.rut = existing.rut || r.clientRut || null;
        existing.clientName = existing.clientName || r.clientName || null;
        existing.branchName = existing.branchName || r.branchName || null;
        existing.dispatchDate = existing.dispatchDate || r.dispatchDate || null;
        existing.externalDocumentID = existing.externalDocumentID || r.externalDocumentID || null;
        existing.mongoCreatedAt = existing.mongoCreatedAt || r.createdAt || null;
        map.set(id, existing);
    }

    return Array.from(map.values());
}

async function getSaleByExternal({ apiKey, externalDocumentID }) {
    const url = `${SALE_BASE}GetSaleByExternalDocumentID?externalDocumentID=${encodeURIComponent(externalDocumentID)}`;
    return getJSON(url, apiKey);
}

async function getAllSIIStates({ apiKey, documentType, folio }) {
    const url = `${SALE_BASE}GetAllSIIStates?documentType=${encodeURIComponent(documentType)}&number=${folio}`;
    return getJSON(url, apiKey);
}

async function main() {
    if (!MONGO_URI) { console.error('MONGO_URI no definido'); process.exit(1); }

    console.log(`[audit] MONGO=${DB_NAME} | docType=${DOC_TYPE} | since=${SINCE || '(todo)'} | limit=${LIMIT || 'todos'}`);

    const records = await getInvoiceIdsFromMongo();
    console.log(`[audit] invoiceIds únicos en Mongo: ${records.length}`);
    const work = LIMIT > 0 ? records.slice(0, LIMIT) : records;
    console.log(`[audit] a procesar: ${work.length}`);

    await fetchApiKey();
    const apiKey = getApiKey();

    const results = [];
    const orphans = [];
    const notFoundInDefontana = [];
    let processed = 0;

    for (const r of work) {
        processed++;
        const out = { ...r };
        try {
            const sale = await getSaleByExternal({ apiKey, externalDocumentID: r.invoiceId });
            if (!sale?.success || !sale?.sale) {
                out.defontanaFound = false;
                out.defontanaMessage = sale?.message || sale?.exceptionMessage || 'no_sale';
                notFoundInDefontana.push(out);
            } else {
                const s = sale.sale;
                out.defontanaFound = true;
                out.folio = s.firstFolio;
                out.documentType = s.documentType;
                out.defontanaStatus = s.status;
                out.defontanaStatusClass = classifyDefontanaStatus(s.status);
                out.emissionDate = s.emissionDate;
                out.total = s.total;
                out.clientFileDefontana = s.clientFile;

                // Consulta histórico SII
                const folio = s.firstFolio;
                const docType = s.documentType || DOC_TYPE;
                if (folio) {
                    const states = await getAllSIIStates({ apiKey, documentType: docType, folio });
                    const arr = states?.states || [];
                    out.siiCount = arr.length;
                    out.siiHistory = arr;
                    const last = arr.length ? arr[arr.length - 1] : null;
                    out.siiLastCode = last?.code || null;
                    out.siiLastDate = last?.date || null;
                    out.siiLastClass = classifySIICode(last?.code);
                    // Huérfano = factura registrada en Mongo + existe en Defontana
                    // pero NO está aprobada por SII o NO está centralizada.
                    const dfClass = out.defontanaStatusClass;
                    const siiClass = out.siiLastClass;
                    const isOk = siiClass === 'aceptado' && dfClass === 'ok';
                    if (!isOk) {
                        out.orphanReason = `defontana=${dfClass}|sii=${siiClass}`;
                        orphans.push(out);
                    }
                }
            }
        } catch (err) {
            out.error = err.message;
        }
        results.push(out);

        if (processed % 10 === 0) {
            console.log(`[audit] ${processed}/${work.length} | huérfanos=${orphans.length} | sin-defontana=${notFoundInDefontana.length}`);
        }
        await sleep(150);
    }

    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const baseDir = process.cwd();
    const orphansFile = path.join(baseDir, `audit_orphans_${stamp}.json`);
    const fullFile = path.join(baseDir, `audit_full_${stamp}.json`);
    const notFoundFile = path.join(baseDir, `audit_notfound_${stamp}.json`);

    fs.writeFileSync(orphansFile, JSON.stringify(orphans, null, 2));
    fs.writeFileSync(fullFile, JSON.stringify(results, null, 2));
    fs.writeFileSync(notFoundFile, JSON.stringify(notFoundInDefontana, null, 2));

    console.log('\n=== RESUMEN ===');
    console.log(`Procesados:            ${results.length}`);
    console.log(`No están en Defontana: ${notFoundInDefontana.length}  -> ${notFoundFile}`);
    console.log(`Encontrados:           ${results.length - notFoundInDefontana.length}`);
    console.log(`HUÉRFANOS (no aprobados/rechazados/pendientes): ${orphans.length}  -> ${orphansFile}`);
    console.log(`Dump completo:         ${fullFile}`);

    // Top resumen consola
    if (orphans.length > 0) {
        console.log('\nPrimeros 20 huérfanos:');
        for (const o of orphans.slice(0, 20)) {
            console.log(`  folio=${o.folio} docType=${o.documentType} externalId=${o.invoiceId} sii=${o.siiLastCode}(${o.siiLastClass}) defontana=${o.defontanaStatus} cliente=${o.rut} emision=${o.emissionDate}`);
        }
    }
}

main().catch(e => { console.error(e); process.exit(1); });
