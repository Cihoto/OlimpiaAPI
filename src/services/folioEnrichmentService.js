// Enriquece el cache folios_meta llamando GetSale folio-a-folio para los faltantes.
// Se ejecuta en background: itera con throttle y guarda lo que va consiguiendo.

import { getSaleByFolio } from './accountingDefontanaService.js';
import { upsertFolioMeta, getMissingFolios } from './mongoFolioMeta.js';

const ENRICH_DELAY_MS = 120;
const ENRICH_BATCH = 50;

let isRunning = false;
let lastRun = null;
let stats = { total: 0, done: 0, errors: 0 };

export function getEnrichmentStatus() {
    return { isRunning, lastRun, ...stats };
}

export async function enrichFolios(apiKey, pairs, { onProgress } = {}) {
    if (isRunning) return { skipped: true, reason: 'already_running' };
    isRunning = true;
    lastRun = new Date();
    stats = { total: 0, done: 0, errors: 0 };

    try {
        const missing = await getMissingFolios(pairs);
        stats.total = missing.length;

        for (let i = 0; i < missing.length; i++) {
            const { folio, docType } = missing[i];
            try {
                const sale = await getSaleByFolio(apiKey, docType, folio);
                if (sale) {
                    await upsertFolioMeta({
                        folio,
                        docType,
                        emissionDate: sale.emissionDate || sale.dateTime || null,
                        total: sale.total ?? sale.affectableTotal ?? null,
                        sellerFileId: sale.sellerFileId || null,
                        clientFile: sale.clientFile || null
                    });
                } else {
                    // Marca con null para no reintentar inmediatamente
                    await upsertFolioMeta({ folio, docType });
                }
                stats.done += 1;
            } catch (err) {
                stats.errors += 1;
            }
            onProgress?.({ ...stats });
            // throttle
            await new Promise(r => setTimeout(r, ENRICH_DELAY_MS));
        }
    } finally {
        isRunning = false;
    }

    return { ok: true, ...stats };
}
