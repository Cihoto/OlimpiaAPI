// Flujo de caja proyectado: distribuye facturas por día (pasado + futuro).
// Por cada día (o semana/mes según granularidad) calcula 5 valores:
//   1. proyectado:  saldo de facturas NO morosas con fecha proyectada en ese día
//   2. abonado:     monto ya pagado de esas mismas facturas (totalOriginal - saldo)
//   3. totalPrevisto = proyectado + abonado
//   4. atrasado:    saldo de facturas MOROSAS cuya fecha original de vencimiento cae en ese día
//   5. totalGeneral = totalPrevisto + atrasado

import { getLatestSnapshot } from './mongoCobranzaSnapshots.js';

function condicionToDays(condicion) {
    if (!condicion) return null;
    const c = String(condicion).toUpperCase().trim();
    if (c === 'CONTADO' || c === 'EFECTIVO' || c === 'PAGO INMEDIATO') return 0;
    const m = /CREDITO(\d+)/i.exec(c) || /(\d+)\s*D[IÍ]AS/i.exec(c);
    if (m) return Number(m[1]);
    return null;
}

function addDays(iso, days) {
    if (!iso) return null;
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return null;
    d.setUTCDate(d.getUTCDate() + days);
    return d.toISOString().substring(0, 10);
}

function toDateKey(iso) {
    if (!iso) return null;
    return new Date(iso).toISOString().substring(0, 10);
}

function projectedCashDate(row) {
    const days = condicionToDays(row.condicionPago);
    if (days != null && row.emissionDate) {
        return addDays(row.emissionDate, days);
    }
    if (row.fechaVencimiento) return toDateKey(row.fechaVencimiento);
    return null;
}

function bucketKey(dateStr, granularity) {
    if (!dateStr) return null;
    const d = new Date(dateStr + 'T00:00:00Z');
    if (granularity === 'day') return d.toISOString().substring(0, 10);
    if (granularity === 'week') {
        const day = d.getUTCDay();
        const monday = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - day));
        return monday.toISOString().substring(0, 10);
    }
    if (granularity === 'month') {
        return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-01`;
    }
    return d.toISOString().substring(0, 10);
}

function enumerateBuckets(fromKey, toKey, granularity) {
    const out = [];
    let d = new Date(fromKey + 'T00:00:00Z');
    const end = new Date(toKey + 'T00:00:00Z');
    while (d <= end) {
        out.push(bucketKey(d.toISOString().substring(0, 10), granularity));
        if (granularity === 'day') d.setUTCDate(d.getUTCDate() + 1);
        else if (granularity === 'week') d.setUTCDate(d.getUTCDate() + 7);
        else if (granularity === 'month') d.setUTCMonth(d.getUTCMonth() + 1);
        else d.setUTCDate(d.getUTCDate() + 1);
    }
    // dedup conservando orden
    return Array.from(new Set(out));
}

/**
 * @param {object} opts
 * @param {number} [opts.horizonteDias=60]    días hacia adelante desde hoy
 * @param {number} [opts.horizonteAtras=90]   días hacia atrás desde hoy
 * @param {'day'|'week'|'month'} [opts.granularity='day']
 */
export async function generateFlujoReport(opts = {}) {
    const { horizonteDias = 60, horizonteAtras = 90, granularity = 'day' } = opts;
    const today = opts.today ? new Date(opts.today) : new Date();
    today.setUTCHours(0, 0, 0, 0);
    const todayKey = today.toISOString().substring(0, 10);

    const snap = await getLatestSnapshot({ type: 'morosas' });
    if (!snap) {
        return {
            generatedAt: new Date().toISOString(),
            snapshotAt: null,
            empty: true,
            summary: {},
            buckets: []
        };
    }

    const rows = snap.rows || [];

    // Rango de fechas
    const fromKey = addDays(todayKey, -horizonteAtras);
    const toKey = addDays(todayKey, horizonteDias);

    // Inicializa todos los buckets en el rango con ceros
    const allBucketKeys = enumerateBuckets(fromKey, toKey, granularity);
    const bucketMap = new Map();
    for (const k of allBucketKeys) {
        bucketMap.set(k, {
            date: k,
            isFuture: new Date(k + 'T00:00:00Z') >= today,
            isToday: k === bucketKey(todayKey, granularity),
            proyectado: 0,
            abonado: 0,
            atrasado: 0,
            countProyectables: 0,
            countAtrasadas: 0,
            byClientProy: {},
            byClientAtras: {}
        });
    }

    // 1) NO MOROSAS (proyectables) → ubicar en fechaProyectada
    for (const r of rows) {
        if (!r.esFactura) continue;
        if (r.esMoroso) continue;
        if (r.saldo == null || r.saldo <= 0) continue;
        const fecha = projectedCashDate(r);
        if (!fecha) continue;
        const key = bucketKey(fecha, granularity);
        const b = bucketMap.get(key);
        if (!b) continue;
        const abonado = (r.totalOriginal != null && r.totalOriginal > r.saldo)
            ? r.totalOriginal - r.saldo : 0;
        b.proyectado += r.saldo;
        b.abonado += abonado;
        b.countProyectables += 1;
        if (!b.byClientProy[r.rut]) b.byClientProy[r.rut] = { rut: r.rut, name: r.clientName, total: 0, count: 0 };
        b.byClientProy[r.rut].total += r.saldo;
        b.byClientProy[r.rut].count += 1;
    }

    // 2) MOROSAS (atrasadas) → ubicar en su fecha original de vencimiento
    for (const r of rows) {
        if (!r.esMoroso) continue;
        if (r.saldo == null || r.saldo <= 0) continue;
        const fecha = toDateKey(r.fechaVencimiento);
        if (!fecha) continue;
        const key = bucketKey(fecha, granularity);
        const b = bucketMap.get(key);
        if (!b) continue; // fuera del rango pasado seleccionado
        b.atrasado += r.saldo;
        b.countAtrasadas += 1;
        if (!b.byClientAtras[r.rut]) b.byClientAtras[r.rut] = { rut: r.rut, name: r.clientName, total: 0, count: 0, diasMora: r.diasMora };
        b.byClientAtras[r.rut].total += r.saldo;
        b.byClientAtras[r.rut].count += 1;
    }

    // Cierra agregados
    const buckets = allBucketKeys.map(k => {
        const b = bucketMap.get(k);
        return {
            ...b,
            totalPrevisto: b.proyectado + b.abonado,
            totalGeneral: b.proyectado + b.abonado + b.atrasado,
            byClientProy: Object.values(b.byClientProy).sort((a, c) => c.total - a.total),
            byClientAtras: Object.values(b.byClientAtras).sort((a, c) => c.total - a.total)
        };
    });

    // Totales agregados (toda la ventana)
    const sum = key => buckets.reduce((s, b) => s + b[key], 0);
    const sumCount = key => buckets.reduce((s, b) => s + b[key], 0);
    const summary = {
        proyectado: sum('proyectado'),
        abonado: sum('abonado'),
        totalPrevisto: sum('proyectado') + sum('abonado'),
        atrasado: sum('atrasado'),
        totalGeneral: sum('proyectado') + sum('abonado') + sum('atrasado'),
        proyectablesCount: sumCount('countProyectables'),
        atrasadasCount: sumCount('countAtrasadas')
    };

    // Detalles para tablas expandibles
    const proyectables = [];
    for (const r of rows) {
        if (!r.esFactura || r.esMoroso) continue;
        if (r.saldo == null || r.saldo <= 0) continue;
        const fecha = projectedCashDate(r);
        if (!fecha) continue;
        const key = bucketKey(fecha, granularity);
        if (!bucketMap.has(key)) continue;
        proyectables.push({ ...r, fechaProyectada: fecha });
    }
    const atrasadas = rows.filter(r => r.esMoroso && r.saldo > 0);

    return {
        generatedAt: new Date().toISOString(),
        snapshotAt: snap.generatedAt,
        empty: false,
        params: { horizonteDias, horizonteAtras, granularity, today: todayKey },
        summary,
        buckets,
        proyectables,
        atrasadas
    };
}
