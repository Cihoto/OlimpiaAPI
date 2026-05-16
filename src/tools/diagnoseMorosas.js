// Diagnóstico: compara GetClientCredit (saldo oficial Defontana) vs mi consolidación
// de GetDocumentsToPay. Muestra distribución por tipo doc, antigüedad y signo.
//
// Uso: node src/tools/diagnoseMorosas.js [maxClients=10]

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fetchApiKey, getApiKey } from '../middleware/auth.js';
import { getClientCredit, getDocumentsToPay } from '../services/accountingDefontanaService.js';
import { getCachedClients } from '../services/clientsSyncService.js';

const MAX = Number(process.argv[2] || 10);

function ageBucket(iso) {
    if (!iso) return 'sin_fecha';
    const days = Math.floor((Date.now() - new Date(iso).getTime()) / 86400000);
    if (days < 0) return 'futuro';
    if (days <= 30) return '0-30';
    if (days <= 90) return '31-90';
    if (days <= 180) return '91-180';
    if (days <= 365) return '181-365';
    if (days <= 730) return '1-2 años';
    return '2+ años';
}

async function main() {
    await fetchApiKey();
    const apiKey = getApiKey();

    const clients = await getCachedClients();
    console.log(`Cache: ${clients.length} clientes\n`);

    const ranking = [];
    const allDocs = [];

    for (let i = 0; i < Math.min(MAX, clients.length); i++) {
        const c = clients[i];
        const rut = c.legalCode || c.fileID;
        try {
            const credit = await getClientCredit(apiKey, rut);
            const dtp = await getDocumentsToPay(apiKey, rut);
            const docs = dtp.documents || [];

            const sumAll = docs.reduce((s, d) => s + (d.amount || 0), 0);
            const sumPos = docs.filter(d => d.amount > 0).reduce((s, d) => s + d.amount, 0);
            const sumNeg = docs.filter(d => d.amount < 0).reduce((s, d) => s + d.amount, 0);

            // Consolidación folio por folio (mi lógica actual)
            const byFolio = new Map();
            for (const d of docs) {
                const k = `${d.idTipoDocumento || ''}:${d.number}`;
                byFolio.set(k, (byFolio.get(k) || 0) + (d.amount || 0));
            }
            const foliosNoCero = Array.from(byFolio.entries()).filter(([_, v]) => Math.abs(v) > 1);

            ranking.push({
                rut,
                name: (c.business || c.name || '').substring(0, 50),
                creditPending: credit?.saldoPendiente ?? null,
                stateClient: credit?.stateClient ?? null,
                docsCount: docs.length,
                sumAll,
                sumPos,
                sumNeg,
                consolidatedNonZero: foliosNoCero.length,
                consolidatedSum: foliosNoCero.reduce((s, [_, v]) => s + v, 0),
                discrepancyVsCredit: credit?.saldoPendiente != null ? (foliosNoCero.reduce((s,[_,v]) => s + v, 0) - credit.saldoPendiente) : null
            });

            // Para análisis por tipo y antigüedad
            for (const d of docs) {
                allDocs.push({
                    rut,
                    docType: d.idTipoDocumento,
                    folio: d.number,
                    amount: d.amount,
                    expirationDate: d.expirationDate,
                    ageBucket: ageBucket(d.expirationDate),
                    sign: d.amount > 0 ? '+' : d.amount < 0 ? '-' : '0'
                });
            }
            console.log(`[${i+1}/${MAX}] ${rut} | ${(c.business||c.name||'').substring(0,40)} | credit=${credit?.saldoPendiente} | docs=${docs.length} | sumNeg=${sumNeg} | sumPos=${sumPos}`);
        } catch (err) {
            console.error(`[${rut}] error:`, err.message);
        }
    }

    // Resumen ranking
    console.log('\n\n===== RANKING TOP 10 POR DEUDA REPORTADA EN GetClientCredit =====');
    ranking.sort((a, b) => (a.creditPending ?? 0) - (b.creditPending ?? 0));
    for (const r of ranking.slice(0, 10)) {
        console.log(JSON.stringify(r));
    }

    // Distribución por tipo de documento
    console.log('\n===== DISTRIBUCIÓN POR docType =====');
    const byType = {};
    for (const d of allDocs) {
        if (!byType[d.docType]) byType[d.docType] = { count: 0, sumAll: 0, sumPos: 0, sumNeg: 0 };
        byType[d.docType].count += 1;
        byType[d.docType].sumAll += d.amount;
        if (d.amount > 0) byType[d.docType].sumPos += d.amount;
        else if (d.amount < 0) byType[d.docType].sumNeg += d.amount;
    }
    console.table(byType);

    // Distribución por antigüedad
    console.log('\n===== DISTRIBUCIÓN POR ANTIGÜEDAD =====');
    const byAge = {};
    for (const d of allDocs) {
        if (!byAge[d.ageBucket]) byAge[d.ageBucket] = { count: 0, sumNeg: 0, sumPos: 0 };
        byAge[d.ageBucket].count += 1;
        if (d.amount < 0) byAge[d.ageBucket].sumNeg += d.amount;
        if (d.amount > 0) byAge[d.ageBucket].sumPos += d.amount;
    }
    console.table(byAge);

    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    fs.writeFileSync(`diagnose_${stamp}.json`, JSON.stringify({ ranking, allDocs }, null, 2));
    console.log(`\nDump: diagnose_${stamp}.json`);
}

main().catch(e => { console.error(e); process.exit(1); });
