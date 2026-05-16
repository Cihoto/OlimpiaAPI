// Wrappers GET-only sobre el módulo Accounting de la API Defontana.
// Todos requieren apiKey (Bearer). No realizan POST/PUT/DELETE.

const ACC_BASE = (process.env.ACCOUNTING_API_URL_PROD || 'https://api.defontana.com/api/Accounting/').replace(/\/+$/, '/');
const SALE_BASE = (process.env.SALE_API_URL || 'https://api.defontana.com/api/Sale/').replace(/\/+$/, '/');

async function getJSON(url, apiKey) {
    const r = await fetch(url, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` }
    });
    const text = await r.text();
    try { return { ok: r.ok, status: r.status, body: JSON.parse(text) }; }
    catch { return { ok: r.ok, status: r.status, body: text }; }
}

// Devuelve { saldoPendiente, montoCredito, stateClient, ... }
export async function getClientCredit(apiKey, rut) {
    const url = `${ACC_BASE}GetClientCredit?fileID=${encodeURIComponent(rut)}`;
    const { body } = await getJSON(url, apiKey);
    return body;
}

// Pagina internamente hasta consumir totalItems. Devuelve { documents, totalItems }.
export async function getDocumentsToPay(apiKey, rut, { itemsPerPage = 100 } = {}) {
    const all = [];
    let page = 0;
    let total = null;
    while (true) {
        const url = `${ACC_BASE}GetDocumentsToPay?fileID=${encodeURIComponent(rut)}&ItemsPerPage=${itemsPerPage}&Page=${page}`;
        const { body } = await getJSON(url, apiKey);
        if (!body?.success) {
            return { success: false, message: body?.message || body?.exceptionMessage || 'error', documents: all };
        }
        const docs = body.documents || [];
        all.push(...docs);
        if (total === null) total = body.totalItems || all.length;
        if (docs.length < itemsPerPage || all.length >= total) break;
        page += 1;
    }
    return { success: true, totalItems: total ?? all.length, documents: all };
}

// Lista de clientes activos. status=1 (activos). Pagina automáticamente.
export async function getAllClients(apiKey, { itemsPerPage = 200, maxPages = 50 } = {}) {
    const all = [];
    for (let pageNumber = 1; pageNumber <= maxPages; pageNumber++) {
        const url = `${SALE_BASE}GetClients?status=1&itemsPerPage=${itemsPerPage}&pageNumber=${pageNumber}`;
        const { body } = await getJSON(url, apiKey);
        if (!body?.success) break;
        const list = body.clientList || [];
        all.push(...list);
        if (list.length < itemsPerPage) break;
    }
    return all;
}

// Datos de un folio puntual desde el módulo Sale (para enriquecer con vendedor, emisión, etc.)
export async function getSaleByFolio(apiKey, documentType, folio) {
    const url = `${SALE_BASE}GetSale?documentType=${encodeURIComponent(documentType)}&number=${folio}`;
    const { body } = await getJSON(url, apiKey);
    return body?.['0'] || body?.sale || null;
}

// Para detectar folios muertos (firma confirmada: XML.success === false).
export async function getXmlOk(apiKey, documentType, folio) {
    const url = `${SALE_BASE}GetXMLDocumentBase64?documentType=${encodeURIComponent(documentType)}&number=${folio}`;
    const { body } = await getJSON(url, apiKey);
    return body?.success === true;
}
