import Router from 'express';
import { generateMorosasReport } from '../services/morosasReportService.js';
import {
    getClientsOrSync,
    refreshClientsCache,
    getCacheStatus,
    getCachedClients
} from '../services/clientsSyncService.js';
import {
    saveSnapshot,
    getLatestSnapshot,
    listSnapshots,
    getSnapshotById
} from '../services/mongoCobranzaSnapshots.js';
import { enrichFolios, getEnrichmentStatus } from '../services/folioEnrichmentService.js';
import { generateFlujoReport } from '../services/flujoReportService.js';
import { generateMorosasExcel } from '../services/morosasExcelService.js';
import { syncFactoring, getFactoringSyncStatus } from '../services/factoringSyncService.js';
import { listAllFactoring, getSyncStatus as getFactoringMetaStatus } from '../services/mongoFactoringCache.js';

const router = Router();

// GET /reports/clients
// Devuelve lista cacheada de clientes (excluye RUTs ignorados).
// ?refresh=true fuerza re-sync desde Defontana.
router.get('/clients', async (req, res) => {
    if (!req.apiKey) return res.status(401).json({ success: false, error: 'No autenticado' });
    const force = String(req.query.refresh || 'false').toLowerCase() === 'true';
    try {
        const status = await getCacheStatus();
        let clients;
        if (force || !status.synced || status.isStale) {
            clients = await getClientsOrSync(req.apiKey, { force: true });
        } else {
            clients = await getCachedClients();
        }
        const cacheStatus = await getCacheStatus();
        res.json({
            success: true,
            clients: clients.map(c => ({
                fileID: c.fileID,
                legalCode: c.legalCode,
                name: c.business || c.name,
                paymentID: c.paymentID
            })),
            count: clients.length,
            cache: cacheStatus
        });
    } catch (error) {
        console.error('[reports/clients] error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// POST /reports/clients/refresh - fuerza re-sync
router.post('/clients/refresh', async (req, res) => {
    if (!req.apiKey) return res.status(401).json({ success: false, error: 'No autenticado' });
    try {
        const result = await refreshClientsCache(req.apiKey);
        res.json({ success: true, ...result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// GET /reports/morosas?maxClients=10&onlyMorosas=true&minDiasMora=0&vendedor=&search=
// Si pasas ?ruts=11.111.111-1,22.222.222-2 filtra por esos clientes (más rápido para demo).
router.get('/morosas', async (req, res) => {
    if (!req.apiKey) return res.status(401).json({ success: false, error: 'No autenticado' });

    const maxClients = req.query.maxClients ? Number(req.query.maxClients) : undefined;
    const rutFilter = req.query.ruts ? String(req.query.ruts).split(',').map(s => s.trim()).filter(Boolean) : undefined;
    const onlyMorosas = String(req.query.onlyMorosas || 'true').toLowerCase() !== 'false';
    const minDiasMora = req.query.minDiasMora ? Number(req.query.minDiasMora) : null;
    const vendedor = req.query.vendedor ? String(req.query.vendedor).trim() : null;
    const search = req.query.search ? String(req.query.search).toLowerCase().trim() : null;
    const bucket = req.query.bucket ? String(req.query.bucket).trim() : null;

    try {
        const report = await generateMorosasReport(req.apiKey, { maxClients, rutFilter });

        let rows = report.rows;
        if (onlyMorosas) rows = rows.filter(r => r.esMoroso);
        if (minDiasMora != null) rows = rows.filter(r => (r.diasMora || 0) >= minDiasMora);
        if (vendedor) rows = rows.filter(r => (r.sellerFileId || '').toUpperCase() === vendedor.toUpperCase());
        if (bucket) rows = rows.filter(r => r.bucket === bucket);
        if (search) {
            rows = rows.filter(r =>
                (r.clientName || '').toLowerCase().includes(search) ||
                (r.rut || '').toLowerCase().includes(search) ||
                String(r.folio).includes(search)
            );
        }

        rows.sort((a, b) => (b.diasMora || 0) - (a.diasMora || 0) || b.saldo - a.saldo);

        res.json({ success: true, ...report, rows });
    } catch (error) {
        console.error('[reports/morosas] error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============ SNAPSHOTS ============
// POST /reports/morosas/snapshot — guarda rows + summary del barrido actual
router.post('/morosas/snapshot', async (req, res) => {
    if (!req.apiKey) return res.status(401).json({ success: false, error: 'No autenticado' });
    try {
        const { rows, summary, source = 'frontend', generatedBy = null, meta = {} } = req.body || {};
        if (!Array.isArray(rows) || !summary) {
            return res.status(400).json({ success: false, error: 'rows[] y summary requeridos' });
        }
        const result = await saveSnapshot({ type: 'morosas', rows, summary, source, generatedBy, meta });
        res.json({ success: true, ...result });
    } catch (error) {
        console.error('[reports/morosas/snapshot] error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// GET /reports/morosas/latest — último snapshot guardado.
// Enriquece on-the-fly con folios_meta (emisión + monto total) y con factoring.
router.get('/morosas/latest', async (req, res) => {
    if (!req.apiKey) return res.status(401).json({ success: false, error: 'No autenticado' });
    try {
        const snap = await getLatestSnapshot({ type: 'morosas' });
        if (!snap) return res.json({ success: true, snapshot: null });

        const { getFolioMetaMap } = await import('../services/mongoFolioMeta.js');
        const { getFactoringMap } = await import('../services/mongoFactoringCache.js');
        const pairs = (snap.rows || []).map(r => ({ folio: r.folio, docType: r.docType }));

        const [metaMap, factoringMap] = await Promise.all([
            getFolioMetaMap(pairs),
            getFactoringMap(pairs).catch(() => new Map())
        ]);

        for (const r of snap.rows || []) {
            const m = metaMap.get(`${r.docType}:${r.folio}`);
            if (m) {
                if (m.emissionDate) r.emissionDate = m.emissionDate;
                if (m.total != null) r.totalOriginal = m.total;
                if (m.sellerFileId && !r.sellerFileId) r.sellerFileId = m.sellerFileId;
            }
            const f = factoringMap.get(`${r.docType}:${r.folio}`);
            if (f) {
                r.factoring = {
                    totalCedido: f.totalCedido || 0,
                    company: f.factoringCompany || null,
                    companyRut: f.factoringRut || null,
                    lastCessionDate: f.lastCessionDate || null,
                    cessions: (f.cessions || []).map(c => ({
                        voucherNumber: c.voucherNumber,
                        fiscalYear: c.fiscalYear,
                        date: c.date,
                        amount: c.amount,
                        company: c.company,
                        companyRut: c.companyRut
                    }))
                };
            }
        }

        res.json({ success: true, snapshot: snap });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// GET /reports/morosas/history — listado sin rows (solo metadata)
router.get('/morosas/history', async (req, res) => {
    if (!req.apiKey) return res.status(401).json({ success: false, error: 'No autenticado' });
    try {
        const list = await listSnapshots({ type: 'morosas', limit: Number(req.query.limit || 30) });
        res.json({ success: true, snapshots: list });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// GET /reports/morosas/snapshot/:id
router.get('/morosas/snapshot/:id', async (req, res) => {
    if (!req.apiKey) return res.status(401).json({ success: false, error: 'No autenticado' });
    try {
        const snap = await getSnapshotById(req.params.id);
        if (!snap) return res.status(404).json({ success: false, error: 'No encontrado' });
        res.json({ success: true, snapshot: snap });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============ ENRICHMENT (emisión + monto total) ============
// POST /reports/morosas/enrich — dispara enrich background (no bloquea)
router.post('/morosas/enrich', async (req, res) => {
    if (!req.apiKey) return res.status(401).json({ success: false, error: 'No autenticado' });
    const { pairs } = req.body || {};
    if (!Array.isArray(pairs)) {
        return res.status(400).json({ success: false, error: 'pairs[] requerido' });
    }
    // Fire and forget — el cliente luego puede consultar status
    enrichFolios(req.apiKey, pairs).catch(err => console.error('[enrich] error:', err));
    res.json({ success: true, accepted: pairs.length });
});

router.get('/morosas/enrich/status', async (req, res) => {
    res.json({ success: true, status: getEnrichmentStatus() });
});

// ============ VISTA 2: FLUJO PROYECTADO ============
// GET /reports/morosas/excel?withFactoring=true|false
// Descarga Excel enriquecido del último snapshot. Por default incluye las 4
// columnas de factoring; pasar withFactoring=false para el formato clásico.
router.get('/morosas/excel', async (req, res) => {
    if (!req.apiKey) return res.status(401).json({ success: false, error: 'No autenticado' });
    try {
        const includeAll = String(req.query.includeAll || 'true').toLowerCase() !== 'false';
        const withFactoring = String(req.query.withFactoring ?? 'true').toLowerCase() !== 'false';
        const { buffer, filename } = await generateMorosasExcel({ includeAll, withFactoring });
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
        res.setHeader('Cache-Control', 'no-cache');
        res.send(buffer);
    } catch (error) {
        console.error('[reports/morosas/excel] error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============ FACTORING ============
// POST /reports/sync/factoring?from=YYYY-MM-DD&to=YYYY-MM-DD&fullReset=true
// Dispara batch que pega a Defontana GetVoucherList?VoucherType=TRASPASOFACTORING
// y llena el cache `folio_factoring`. Fire and forget.
router.post('/sync/factoring', async (req, res) => {
    if (!req.apiKey) return res.status(401).json({ success: false, error: 'No autenticado' });
    const from = req.query.from ? String(req.query.from) : undefined;
    const to = req.query.to ? String(req.query.to) : undefined;
    const fullReset = String(req.query.fullReset || 'false').toLowerCase() === 'true';

    const current = getFactoringSyncStatus();
    if (current.isRunning) {
        return res.json({ success: true, started: false, reason: 'already_running', progress: current.progress });
    }
    // Fire and forget
    syncFactoring(req.apiKey, { from, to, fullReset })
        .catch(err => console.error('[factoring sync] error:', err));
    res.json({ success: true, started: true, from: from || 'auto', to: to || 'auto', fullReset });
});

// GET /reports/sync/factoring/status
router.get('/sync/factoring/status', async (req, res) => {
    try {
        const live = getFactoringSyncStatus();
        const meta = await getFactoringMetaStatus();
        res.json({ success: true, live, meta });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// GET /reports/sync/factoring/list — dump del cache para inspección
router.get('/sync/factoring/list', async (req, res) => {
    try {
        const docs = await listAllFactoring();
        res.json({ success: true, count: docs.length, items: docs });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// GET /reports/flujo?horizonte=60&atras=90&granularity=day
router.get('/flujo', async (req, res) => {
    if (!req.apiKey) return res.status(401).json({ success: false, error: 'No autenticado' });
    try {
        const horizonteDias = Number(req.query.horizonte || 60);
        const horizonteAtras = Number(req.query.atras || 90);
        const granularity = String(req.query.granularity || 'day');
        const result = await generateFlujoReport({ horizonteDias, horizonteAtras, granularity });
        res.json({ success: true, ...result });
    } catch (error) {
        console.error('[reports/flujo] error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

export default router;
