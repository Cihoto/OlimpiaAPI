// Servicio de descubrimiento: escanea Defontana para encontrar TODOS los voucher
// types que podrían contener cesiones de factoring.
//
// Estrategia:
//   1. Defontana no expone "listar voucher types disponibles" como tal, pero sí
//      podemos probar una lista heurística de tipos plausibles y ver cuáles
//      devuelven resultados.
//   2. Para cada voucher type que devuelve >0 vouchers, leer una muestra del
//      detalle y contar cuántas líneas parecen ser cesiones (tienen
//      documentNumber + documentType + credit > 0).
//   3. Devolver un reporte con: voucherType, total de vouchers, total de líneas
//      de cesión, monto sumado, gloss típico, RUT contraparte si detectable.
//
// Esto le da al usuario evidencia concreta sobre qué voucher types incluir en
// el sync. No modifica datos — sólo inspecciona.

const ACC_BASE = (process.env.ACCOUNTING_API_URL_PROD || 'https://api.defontana.com/api/Accounting/').replace(/\/+$/, '/');

// Lista de voucher types plausibles para cesiones a financieras / factoring.
// Si el usuario sabe de otros, puede agregarlos por env var.
const CANDIDATE_TYPES_DEFAULT = [
    'TRASPASOFACTORING',
    'CESIONFACTORING',
    'FACTORING',
    'TRASPASOFACT',
    'CESION',
    'CONFIRMING',
    'TRASPASOBANCO',
    'CESIONBANCO',
    'ANTICIPOFACTORING'
];

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function getJSON(url, apiKey) {
    const r = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' } });
    const text = await r.text();
    try { return { ok: r.ok, status: r.status, body: JSON.parse(text) }; }
    catch { return { ok: r.ok, status: r.status, body: text }; }
}

/**
 * @param {string} apiKey
 * @param {object} opts
 * @param {string} [opts.from] YYYY-MM-DD (default: hace 90 días)
 * @param {string} [opts.to]   YYYY-MM-DD (default: hoy)
 * @param {string[]} [opts.candidates] tipos extra a probar (además de los default)
 * @param {number} [opts.sampleSize=3] cuántos vouchers leer por tipo
 */
export async function discoverFactoringVoucherTypes(apiKey, opts = {}) {
    const today = new Date();
    const ago = new Date(); ago.setDate(ago.getDate() - 90);
    const from = opts.from || ago.toISOString().substring(0, 10);
    const to = opts.to || today.toISOString().substring(0, 10);
    const candidates = [...new Set([...(CANDIDATE_TYPES_DEFAULT), ...(opts.candidates || [])])];
    const sampleSize = opts.sampleSize ?? 3;

    const results = [];
    for (const type of candidates) {
        const listUrl = `${ACC_BASE}GetVoucherList?VoucherType=${encodeURIComponent(type)}&FromDate=${from}&ToDate=${to}&ItemsPerPage=10&Page=0`;
        const { body, status } = await getJSON(listUrl, apiKey);
        const list = body?.vouchers || body?.items || body?.voucherList || [];
        const apiOk = body?.success !== false;
        const totalDocs = body?.totalItems ?? list.length;

        const result = {
            voucherType: type,
            apiOk,
            httpStatus: status,
            errorMessage: !apiOk ? (body?.message || body?.exceptionMessage || null) : null,
            vouchersInRange: totalDocs,
            sampleVouchers: [],
            cessionLineCount: 0,
            cessionTotalAmount: 0,
            sampleGlosses: [],
            sampleCompanyRuts: new Set()
        };

        // Si hubo vouchers, leer una muestra para ver si tienen líneas de cesión
        const sample = list.slice(0, sampleSize);
        for (const v of sample) {
            const vt = v.voucherType || v.type || type;
            const num = v.number ?? v.voucherNumber ?? v.id;
            const fy = v.fiscalYear || v.year || (new Date(v.date || Date.now())).getUTCFullYear();
            const detailUrl = `${ACC_BASE}GetVoucher?VoucherType=${encodeURIComponent(vt)}&Number=${num}&FiscalYear=${fy}`;
            const { body: det } = await getJSON(detailUrl, apiKey);
            if (!det?.header || !det?.detail) continue;

            const gloss = det.header.comment || det.header.gloss || v.gloss || '';
            if (gloss && result.sampleGlosses.length < 5) result.sampleGlosses.push(gloss);

            let lineasCesion = 0;
            let totalLineas = 0;
            for (const line of det.detail) {
                if (line.documentType && line.documentNumber && (line.credit || 0) > 0) {
                    lineasCesion += 1;
                    totalLineas += line.credit;
                }
                // Recolectar posibles RUTs de contraparte del propio detalle
                const rutFromAcc = (line.accountName || '').match(/\d{1,2}\.?\d{3}\.?\d{3}-?[\dKk]/);
                if (rutFromAcc) result.sampleCompanyRuts.add(rutFromAcc[0]);
            }
            result.cessionLineCount += lineasCesion;
            result.cessionTotalAmount += totalLineas;
            result.sampleVouchers.push({
                number: num, fiscalYear: fy, gloss, totalLines: det.detail.length, cessionLines: lineasCesion
            });
            await sleep(80);
        }
        result.sampleCompanyRuts = [...result.sampleCompanyRuts];
        results.push(result);
        await sleep(120);
    }

    return {
        from, to,
        candidatesTested: candidates.length,
        results: results.sort((a, b) => b.vouchersInRange - a.vouchersInRange)
    };
}
