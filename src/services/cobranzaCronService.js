// Cron interno (in-process) para mantenimiento diario del reporte de cobranza.
// Corre dentro del mismo servidor Express — no requiere cron externo en Render.
//
// Ciclo (default: cada 24h, primera corrida 6 AM Chile):
//   1. Sync factoring incremental
//   2. Sweep XML SII (folios muertos)
//   3. Regen snapshot completo
//
// Patrón idéntico a startDeliveryCapacityCleanupCron.

import { fetchApiKey, getApiKey } from '../middleware/auth.js';
import { syncFactoring } from './factoringSyncService.js';
import { sweepDeadFromLatestSnapshot } from './deadFoliosSweepService.js';
import { generateMorosasReport } from './morosasReportService.js';
import { saveSnapshot } from './mongoCobranzaSnapshots.js';
import { listClients } from './mongoClientsCache.js';

let isRunning = false;
let lastRunAt = null;
let lastResult = null;

export function getCobranzaCronStatus() {
    return { isRunning, lastRunAt, lastResult };
}

/**
 * Una corrida del ciclo de mantenimiento.
 */
export async function runCobranzaMaintenance({ logger = console } = {}) {
    if (isRunning) return { skipped: true, reason: 'already_running' };
    isRunning = true;
    const start = Date.now();
    const tag = (s) => logger.log(`[cobranzaCron ${new Date().toISOString().slice(11, 19)}] ${s}`);

    try {
        tag('=== INICIO MANTENIMIENTO ===');
        await fetchApiKey();
        const apiKey = getApiKey();

        // 1) Factoring incremental
        tag('1/3 sync factoring (incremental)…');
        const factResult = await syncFactoring(apiKey, { fullReset: false });
        tag(`  factoring: ${factResult.cesionesEncontradas || 0} cesiones · skipped: ${factResult.skipped || 0}`);

        // 2) Sweep XML SII
        tag('2/3 sweep folios muertos…');
        const sweep = await sweepDeadFromLatestSnapshot(apiKey, { source: 'cron-internal' });
        tag(`  sweep: ${sweep.deadFound || 0} muertos · ${sweep.transientErrors || 0} err transitorios`);

        // 3) Regen snapshot
        tag('3/3 regenerando snapshot…');
        const clients = await listClients();
        const ruts = clients.map(c => c.legalCode || c.fileID).filter(Boolean);

        const allRows = [];
        const CHUNK = 25;
        for (let i = 0; i < ruts.length; i += CHUNK) {
            const batch = ruts.slice(i, i + CHUNK);
            const results = await Promise.allSettled(
                batch.map(rut => generateMorosasReport(apiKey, { rutFilter: [rut] }))
            );
            for (const r of results) {
                if (r.status === 'fulfilled' && r.value?.rows) allRows.push(...r.value.rows);
            }
        }

        const morosas = allRows.filter(r => r.esMoroso);
        const vencenHoy = allRows.filter(r => r.venceHoy);
        const buckets = { '1-30': { count: 0, total: 0 }, '31-60': { count: 0, total: 0 }, '61-90': { count: 0, total: 0 }, '90+': { count: 0, total: 0 } };
        for (const r of morosas) {
            if (buckets[r.bucket]) { buckets[r.bucket].count++; buckets[r.bucket].total += r.saldo; }
        }
        const summary = {
            morosasCount: morosas.length,
            morosasTotal: morosas.reduce((s, r) => s + r.saldo, 0),
            vencenHoyCount: vencenHoy.length,
            vencenHoyTotal: vencenHoy.reduce((s, r) => s + r.saldo, 0),
            buckets
        };

        await saveSnapshot({
            type: 'morosas', rows: allRows, summary,
            source: 'cron-internal', generatedBy: 'system',
            meta: { generatedAt: new Date().toISOString(), elapsedSec: Math.round((Date.now() - start) / 1000) }
        });

        const dur = Math.round((Date.now() - start) / 1000);
        tag(`snapshot: ${allRows.length} filas · ${morosas.length} morosas · $${summary.morosasTotal.toLocaleString('es-CL')}`);
        tag(`=== FIN (${dur}s) ===`);

        lastResult = {
            ok: true, elapsedSec: dur,
            factoring: factResult.cesionesEncontradas || 0,
            deadDetected: sweep.deadFound || 0,
            morosasCount: morosas.length,
            morosasTotal: summary.morosasTotal
        };
        return { success: true, ...lastResult };
    } catch (error) {
        logger.error('[cobranzaCron] error:', error);
        lastResult = { ok: false, error: error.message };
        return { success: false, error: error.message };
    } finally {
        isRunning = false;
        lastRunAt = new Date();
    }
}

/**
 * Arranca el cron in-process.
 * Por default: corre 1×/día. Primera corrida demorada hasta las 6 AM hora Chile
 * (o inmediato si COBRANZA_CRON_RUN_ON_START=true).
 */
export function startCobranzaCron({
    intervalHours = 24,
    runOnStart = false,
    targetHourChile = 6,
    logger = console
} = {}) {
    const intervalMs = intervalHours * 60 * 60 * 1000;

    const tick = () => {
        runCobranzaMaintenance({ logger }).catch(err => logger.error('[cobranzaCron] tick error:', err));
    };

    if (runOnStart) {
        logger.log('[cobranzaCron] arrancando corrida inmediata');
        tick();
    }

    // Calcular ms hasta la próxima hora objetivo (6 AM Chile = 10 AM UTC en horario estándar / 09 UTC en horario verano).
    // Para simplificar usamos UTC offset -4 (CLT) que es el más común.
    const now = new Date();
    const target = new Date(now);
    target.setUTCHours((targetHourChile + 4) % 24, 0, 0, 0); // 06:00 Chile = 10:00 UTC
    if (target <= now) target.setUTCDate(target.getUTCDate() + 1); // mañana
    const msUntilFirst = target.getTime() - now.getTime();
    logger.log(`[cobranzaCron] primera corrida programada para ${target.toISOString()} (en ${Math.round(msUntilFirst / 60000)} min)`);

    const firstTimer = setTimeout(() => {
        tick();
        // A partir de ahí, intervalo regular
        setInterval(tick, intervalMs);
    }, msUntilFirst);

    return {
        stop: () => clearTimeout(firstTimer),
        runNow: tick
    };
}
