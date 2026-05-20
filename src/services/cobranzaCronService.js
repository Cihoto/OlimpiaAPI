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
let progress = { stage: 'idle', label: 'Inactivo', percent: 0 };

export function getCobranzaCronStatus() {
    return { isRunning, progress, lastRunAt, lastResult };
}

function setProgress(stage, label, percent) {
    progress = { stage, label, percent: Math.round(percent) };
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
        setProgress('starting', 'Conectando con Defontana…', 1);
        await fetchApiKey();
        const apiKey = getApiKey();

        // 1) Factoring incremental
        tag('1/3 sync factoring (incremental)…');
        setProgress('factoring', 'Sincronizando factoring…', 5);
        const factResult = await syncFactoring(apiKey, { fullReset: false });
        tag(`  factoring: ${factResult.cesionesEncontradas || 0} cesiones · skipped: ${factResult.skipped || 0}`);

        // 2) Sweep XML SII
        tag('2/3 sweep folios muertos…');
        setProgress('sweep', 'Detectando facturas muertas…', 25);
        const sweep = await sweepDeadFromLatestSnapshot(apiKey, { source: 'cron-internal' });
        tag(`  sweep: ${sweep.deadFound || 0} muertos · ${sweep.transientErrors || 0} err transitorios`);

        // 3) Regen snapshot
        tag('3/3 regenerando snapshot…');
        setProgress('snapshot', 'Regenerando reporte de cobranza…', 45);
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
            // El regen ocupa el rango 45%-95% del total
            const ratio = Math.min(1, (i + CHUNK) / ruts.length);
            setProgress('snapshot', `Regenerando reporte · ${Math.min(i + CHUNK, ruts.length)}/${ruts.length} clientes`, 45 + ratio * 50);
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

        setProgress('saving', 'Guardando reporte…', 96);
        await saveSnapshot({
            type: 'morosas', rows: allRows, summary,
            source: 'cron-internal', generatedBy: 'system',
            meta: { generatedAt: new Date().toISOString(), elapsedSec: Math.round((Date.now() - start) / 1000) }
        });

        const dur = Math.round((Date.now() - start) / 1000);
        tag(`snapshot: ${allRows.length} filas · ${morosas.length} morosas · $${summary.morosasTotal.toLocaleString('es-CL')}`);
        tag(`=== FIN (${dur}s) ===`);
        setProgress('done', 'Completado', 100);

        lastResult = {
            ok: true, elapsedSec: dur,
            factoring: factResult.cesionesEncontradas || 0,
            deadDetected: sweep.deadFound || 0,
            morosasCount: morosas.length,
            morosasTotal: summary.morosasTotal,
            generatedAt: new Date().toISOString()
        };
        return { success: true, ...lastResult };
    } catch (error) {
        logger.error('[cobranzaCron] error:', error);
        setProgress('error', 'Error: ' + error.message, 0);
        lastResult = { ok: false, error: error.message };
        return { success: false, error: error.message };
    } finally {
        isRunning = false;
        lastRunAt = new Date();
    }
}

/**
 * Arranca el cron in-process. Corre cada `intervalHours` (default 4h).
 * La primera corrida es a las `intervalHours` del arranque, salvo runOnStart=true.
 */
export function startCobranzaCron({
    intervalHours = 4,
    runOnStart = false,
    logger = console
} = {}) {
    const intervalMs = intervalHours * 60 * 60 * 1000;

    const tick = () => {
        runCobranzaMaintenance({ logger }).catch(err => logger.error('[cobranzaCron] tick error:', err));
    };

    if (runOnStart) {
        logger.log('[cobranzaCron] corrida inmediata al arranque');
        tick();
    }

    logger.log(`[cobranzaCron] programado cada ${intervalHours}h`);
    const timer = setInterval(tick, intervalMs);

    return {
        stop: () => clearInterval(timer),
        runNow: tick
    };
}
