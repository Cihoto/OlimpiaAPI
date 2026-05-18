// Excel premium con múltiples hojas — diseño minimalista profesional.
//
// Hojas:
//   1) Morosa           — facturas vencidas con saldo > 0 (la principal)
//   2) Resumen          — KPIs y top deudores
//   3) Vence hoy        — facturas que vencen hoy
//   4) Todo detallado   — solo morosas + vence hoy con tags de categoría
//
// Paleta minimalista: zinc-900 headers + acentos semánticos
// (rose para morosidad, amber para hoy, indigo para factoring).

import ExcelJS from 'exceljs';
import { getLatestSnapshot } from './mongoCobranzaSnapshots.js';
import { getFolioMetaMap } from './mongoFolioMeta.js';
import { listClients } from './mongoClientsCache.js';
import { getFactoringMap } from './mongoFactoringCache.js';

// ============ HELPERS ============

const COLORS = {
    headerBg: 'FF18181B',        // zinc-900
    headerFg: 'FFFFFFFF',
    rowAltBg: 'FFFAFAFA',         // zinc-50
    borderLight: 'FFE4E4E7',      // zinc-200
    rose: 'FFE11D48',             // rose-600
    roseSoft: 'FFFFE4E6',         // rose-100
    amber: 'FFD97706',            // amber-600
    amberSoft: 'FFFEF3C7',        // amber-100
    emerald: 'FF059669',          // emerald-600
    emeraldSoft: 'FFD1FAE5',      // emerald-100
    indigo: 'FF4F46E5',           // indigo-600
    indigoSoft: 'FFE0E7FF',       // indigo-100
    zinc500: 'FF71717A',
    zinc700: 'FF3F3F46',
};

const CATEGORY_THEME = {
    'Morosa': { bg: COLORS.roseSoft, fg: COLORS.rose, accent: 'FFFEF2F2' },
    'Vence hoy': { bg: COLORS.amberSoft, fg: COLORS.amber, accent: 'FFFFFBEB' },
    'Crédito a favor': { bg: COLORS.emeraldSoft, fg: COLORS.emerald, accent: 'FFECFDF5' },
    'Pendiente (no vencida)': { bg: COLORS.indigoSoft, fg: COLORS.indigo, accent: 'FFEEF2FF' },
};

function classifyCategory(row) {
    if (row.esMoroso) return 'Morosa';
    if (row.venceHoy) return 'Vence hoy';
    if (row.esCreditoCliente) return 'Crédito a favor';
    if (row.esFactura) return 'Pendiente (no vencida)';
    return 'Otro';
}

function bucketLabel(diasMora) {
    if (diasMora == null || diasMora <= 0) return '—';
    if (diasMora <= 30) return '1-30';
    if (diasMora <= 60) return '31-60';
    if (diasMora <= 90) return '61-90';
    return '90+';
}

function applyHeaderRow(ws, row) {
    row.eachCell({ includeEmpty: true }, (cell) => {
        cell.font = { bold: true, color: { argb: COLORS.headerFg }, size: 11 };
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.headerBg } };
        cell.alignment = { vertical: 'middle', horizontal: 'left', indent: 0 };
        cell.border = {
            bottom: { style: 'thin', color: { argb: COLORS.headerBg } }
        };
    });
    row.height = 26;
}

function applyZebraStripes(ws, startRow, endRow, startCol = 1, endCol) {
    const lastCol = endCol || ws.columnCount;
    for (let r = startRow; r <= endRow; r++) {
        const row = ws.getRow(r);
        if ((r - startRow) % 2 === 1) {
            for (let c = startCol; c <= lastCol; c++) {
                const cell = row.getCell(c);
                cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.rowAltBg } };
            }
        }
    }
}

function applyBorders(ws, startRow, endRow, startCol = 1, endCol) {
    const lastCol = endCol || ws.columnCount;
    for (let r = startRow; r <= endRow; r++) {
        const row = ws.getRow(r);
        for (let c = startCol; c <= lastCol; c++) {
            row.getCell(c).border = {
                bottom: { style: 'thin', color: { argb: COLORS.borderLight } }
            };
        }
    }
}

// Convierte índice de columna (1) a letra Excel ('A', 'B'... 'AA')
function colLetter(idx) {
    let s = '';
    while (idx > 0) {
        const m = (idx - 1) % 26;
        s = String.fromCharCode(65 + m) + s;
        idx = Math.floor((idx - 1) / 26);
    }
    return s;
}

// Genera barra visual con caracteres unicode (█ + ░ proporcional al ratio).
// Más robusto que dataBar de ExcelJS que rompe Excel por bug del extLst.
function unicodeBar(value, max, width = 14) {
    if (max <= 0) return '';
    const filled = Math.max(0, Math.min(width, Math.round((value / max) * width)));
    return '█'.repeat(filled) + '░'.repeat(width - filled);
}

// ============ HOJA RESUMEN ============

function buildResumenSheet(wb, snap, rows) {
    const ws = wb.addWorksheet('Resumen', {
        views: [{ showGridLines: false }],
        properties: { defaultColWidth: 18 }
    });

    // Title
    ws.mergeCells('A1:F1');
    const title = ws.getCell('A1');
    title.value = 'Reporte de Cobranza';
    title.font = { bold: true, size: 22, color: { argb: COLORS.headerBg } };
    title.alignment = { vertical: 'middle' };
    ws.getRow(1).height = 36;

    ws.mergeCells('A2:F2');
    const sub = ws.getCell('A2');
    const fechaGen = snap.generatedAt ? new Date(snap.generatedAt) : new Date();
    sub.value = `Generado ${fechaGen.toLocaleString('es-CL')} · Usuario: ${snap.generatedBy || '—'}`;
    sub.font = { size: 10, color: { argb: COLORS.zinc500 } };
    ws.getRow(2).height = 18;

    // Categorías
    const byCat = {};
    for (const r of rows) {
        const cat = classifyCategory(r);
        if (!byCat[cat]) byCat[cat] = { count: 0, total: 0 };
        byCat[cat].count += 1;
        byCat[cat].total += r.saldo || 0;
    }

    // KPI cards (row 4..7, 3 columnas)
    const kpiTitles = ['Total moroso', 'Vencen hoy', 'Total filas'];
    const kpiValues = [
        byCat['Morosa']?.total ?? 0,
        byCat['Vence hoy']?.total ?? 0,
        rows.length
    ];
    const kpiCounts = [
        byCat['Morosa']?.count ?? 0,
        byCat['Vence hoy']?.count ?? 0,
        null
    ];
    const kpiColors = [COLORS.rose, COLORS.amber, COLORS.zinc700];

    ws.getRow(4).height = 8;
    for (let i = 0; i < kpiTitles.length; i++) {
        const c = i + 1;
        const labelCell = ws.getCell(5, c);
        labelCell.value = kpiTitles[i];
        labelCell.font = { size: 9, bold: true, color: { argb: COLORS.zinc500 } };
        labelCell.alignment = { vertical: 'middle' };

        const valueCell = ws.getCell(6, c);
        valueCell.value = kpiValues[i];
        valueCell.font = { size: 18, bold: true, color: { argb: kpiColors[i] } };
        valueCell.numFmt = i === kpiTitles.length - 1 ? '#,##0' : '"$"#,##0';

        const countCell = ws.getCell(7, c);
        countCell.value = kpiCounts[i] != null ? `${kpiCounts[i]} facturas` : '';
        countCell.font = { size: 9, color: { argb: COLORS.zinc500 } };

        // bordes laterales sutiles entre KPIs
        for (let r = 5; r <= 7; r++) {
            ws.getCell(r, c).border = {
                top: { style: 'thin', color: { argb: COLORS.borderLight } },
                bottom: { style: 'thin', color: { argb: COLORS.borderLight } },
                left: c === 1 ? { style: 'thin', color: { argb: COLORS.borderLight } } : undefined,
                right: { style: 'thin', color: { argb: COLORS.borderLight } }
            };
        }
    }
    ws.getRow(5).height = 18;
    ws.getRow(6).height = 28;
    ws.getRow(7).height = 16;

    // ================ Sección: Distribución por antigüedad ================
    let r = 10;
    ws.getCell(`A${r}`).value = 'Distribución por antigüedad';
    ws.getCell(`A${r}`).font = { bold: true, size: 13, color: { argb: COLORS.headerBg } };
    r += 2;

    const buckets = ['1-30', '31-60', '61-90', '90+'];
    const morosas = rows.filter(x => x.esMoroso);
    const bucketData = buckets.map(b => {
        const items = morosas.filter(x => bucketLabel(x.diasMora) === b);
        return { bucket: b, count: items.length, total: items.reduce((s, x) => s + x.saldo, 0) };
    });
    const totalMoroso = morosas.reduce((s, x) => s + x.saldo, 0);

    const headerRow = ws.getRow(r);
    headerRow.values = ['Bucket', 'Facturas', 'Monto', '% del total', 'Visual'];
    applyHeaderRow(ws, headerRow);
    r += 1;
    const distStart = r;
    const maxBucketTotal = Math.max(...bucketData.map(b => b.total), 1);
    for (const b of bucketData) {
        const pct = totalMoroso > 0 ? b.total / totalMoroso : 0;
        const row = ws.getRow(r);
        row.values = [b.bucket, b.count, b.total, pct, unicodeBar(b.total, maxBucketTotal, 16)];
        row.getCell(1).font = { bold: true };
        row.getCell(3).numFmt = '"$"#,##0';
        row.getCell(4).numFmt = '0.0%';
        const visual = row.getCell(5);
        visual.font = { name: 'Consolas', size: 11, color: { argb: COLORS.rose } };
        visual.alignment = { horizontal: 'left' };
        r += 1;
    }
    applyZebraStripes(ws, distStart, r - 1, 1, 5);
    applyBorders(ws, distStart, r - 1, 1, 5);
    ws.getColumn(5).width = 22;

    r += 2;
    // ================ Sección: Top 15 clientes deudores ================
    ws.getCell(`A${r}`).value = 'Top 15 clientes deudores';
    ws.getCell(`A${r}`).font = { bold: true, size: 13, color: { argb: COLORS.headerBg } };
    r += 2;

    const byClient = {};
    for (const m of morosas) {
        const k = m.rut;
        if (!byClient[k]) byClient[k] = { rut: m.rut, name: m.clientName, total: 0, count: 0, maxDiasMora: 0 };
        byClient[k].total += m.saldo;
        byClient[k].count += 1;
        if ((m.diasMora ?? 0) > byClient[k].maxDiasMora) byClient[k].maxDiasMora = m.diasMora ?? 0;
    }
    const top15 = Object.values(byClient).sort((a, b) => b.total - a.total).slice(0, 15);

    const topHeader = ws.getRow(r);
    topHeader.values = ['Cliente', 'RUT', 'Facturas', 'Máx. días mora', 'Monto', 'Visual'];
    applyHeaderRow(ws, topHeader);
    r += 1;
    const topStart = r;
    const maxClientTotal = Math.max(...top15.map(c => c.total), 1);
    for (const c of top15) {
        const row = ws.getRow(r);
        row.values = [c.name || '—', c.rut, c.count, c.maxDiasMora, c.total, unicodeBar(c.total, maxClientTotal, 16)];
        row.getCell(2).font = { name: 'Consolas', size: 10, color: { argb: COLORS.zinc500 } };
        row.getCell(5).numFmt = '"$"#,##0';
        const visual = row.getCell(6);
        visual.font = { name: 'Consolas', size: 11, color: { argb: COLORS.indigo } };
        visual.alignment = { horizontal: 'left' };
        r += 1;
    }
    applyZebraStripes(ws, topStart, r - 1, 1, 6);
    applyBorders(ws, topStart, r - 1, 1, 6);

    // Anchos óptimos
    ws.getColumn(1).width = 42;
    ws.getColumn(2).width = 16;
    ws.getColumn(3).width = 12;
    ws.getColumn(4).width = 16;
    ws.getColumn(5).width = 18;
    ws.getColumn(6).width = 28;
}

// ============ HOJAS DE CATEGORÍA ============

const COLUMNS_BASE = [
    { key: 'rut',              header: 'RUT',              width: 14, type: 'text' },
    { key: 'clientName',       header: 'Razón social',     width: 44, type: 'text' },
    { key: 'giro',             header: 'Giro',             width: 36, type: 'text' },
    { key: 'comuna',           header: 'Comuna',           width: 18, type: 'text' },
    { key: 'region',           header: 'Región',           width: 22, type: 'text' },
    { key: 'email',            header: 'Email',            width: 26, type: 'text' },
    { key: 'sellerFileId',     header: 'Vendedor',         width: 12, type: 'text' },
    { key: 'condicionPago',    header: 'Cond. pago',       width: 14, type: 'text' },
    { key: 'folio',            header: 'Folio',            width: 10, type: 'text' },
    { key: 'docType',          header: 'Tipo doc',         width: 13, type: 'text' },
    { key: 'codeSii',          header: 'Cód. SII',         width: 10, type: 'text' },
    { key: 'emissionDate',     header: 'Emisión',          width: 13, type: 'date' },
    { key: 'fechaVencimiento', header: 'Vencimiento',      width: 13, type: 'date' },
    { key: 'diasMora',         header: 'Días mora',        width: 11, type: 'number' },
    { key: 'bucket',           header: 'Bucket',           width: 11, type: 'text' },
    { key: 'totalOriginal',    header: 'Monto total',      width: 16, type: 'money' },
    { key: 'abonado',          header: 'Abonado',          width: 16, type: 'money' },
    { key: 'saldo',            header: 'Saldo',            width: 16, type: 'money' }
];

// Convención conceptual (validada vs CSV Antonio):
//   - 'saldo' (col base) = lo que el cliente debe a Olimpia, NETO del anticipo.
//   - 'anticipoFactor'   = monto que la financiera ya adelantó a Olimpia.
//   - 'totalFacturado'   = saldo + anticipoFactor = monto original de la factura
//                          (es el "Debe" del CSV de Antonio).
// La columna anterior "Saldo propio" se eliminó: era saldo - cedido, lo que daba
// $0 falso para casos como CENCOSUD 16615 donde el cliente sí debe el saldo neto.
const COLUMNS_FACTORING = [
    { key: 'anticipoFactor',   header: 'Anticipo del factor', width: 19, type: 'money', factoring: true },
    { key: 'empresaFactoring', header: 'Empresa factor',      width: 26, type: 'text',  factoring: true },
    { key: 'fechaCesion',      header: 'Fecha cesión',        width: 13, type: 'date',  factoring: true },
    { key: 'totalFacturado',   header: 'Total facturado',     width: 18, type: 'money', factoring: true }
];

function buildColumns(withFactoring) {
    return withFactoring ? [...COLUMNS_BASE, ...COLUMNS_FACTORING] : COLUMNS_BASE;
}

function rowFromData(r, clientsMap, folioMetaMap, factoringMap) {
    const meta = folioMetaMap.get(`${r.docType}:${r.folio}`);
    const client = clientsMap.get(r.rut) || {};
    const fact = factoringMap?.get(`${r.docType}:${r.folio}`);
    const emission = r.emissionDate || meta?.emissionDate || null;
    const total = r.totalOriginal ?? meta?.total ?? null;
    const saldo = r.saldo ?? 0;
    const abonado = total != null ? Math.max(0, total - saldo) : null;
    const anticipoFactor = fact?.totalCedido ?? 0;
    // Total facturado = saldo (neto que cliente debe) + anticipo del factor que Olimpia ya recibió.
    // Si la factura no tuvo factoring, anticipoFactor=0 y totalFacturado=saldo.
    const totalFacturado = saldo + anticipoFactor;
    return {
        rut: r.rut || '',
        clientName: r.clientName || client.name || client.business || '',
        giro: r.giro || client.business || '',
        comuna: client.district || '',
        region: client.state || '',
        email: client.email || '',
        sellerFileId: r.sellerFileId || client.sellerID || '',
        condicionPago: r.condicionPago || client.paymentID || '',
        folio: r.folio,
        docType: r.docType || '',
        codeSii: r.codeSii || '',
        emissionDate: emission ? new Date(emission) : null,
        fechaVencimiento: r.fechaVencimiento ? new Date(r.fechaVencimiento) : null,
        diasMora: r.diasMora,
        bucket: bucketLabel(r.diasMora),
        totalOriginal: total,
        abonado,
        saldo,
        anticipoFactor,
        empresaFactoring: fact?.factoringCompany || '',
        fechaCesion: fact?.lastCessionDate ? new Date(fact.lastCessionDate) : null,
        totalFacturado
    };
}

function buildCategorySheet(wb, sheetName, rows, category, clientsMap, folioMetaMap, factoringMap, columns) {
    const filtered = category
        ? rows.filter(r => classifyCategory(r) === category)
        : rows;

    const ws = wb.addWorksheet(sheetName, {
        views: [{ state: 'frozen', ySplit: 4, showGridLines: false }]
    });

    const theme = CATEGORY_THEME[category] || { bg: COLORS.indigoSoft, fg: COLORS.indigo };

    const lastColLetter = colLetter(columns.length);
    const diasMoraIdx = columns.findIndex(c => c.key === 'diasMora') + 1; // 1-based
    const anticipoIdx = columns.findIndex(c => c.key === 'anticipoFactor') + 1;
    const empresaIdx = columns.findIndex(c => c.key === 'empresaFactoring') + 1;
    const totalFactIdx = columns.findIndex(c => c.key === 'totalFacturado') + 1;

    // ======= Encabezado de la hoja (4 filas) =======
    ws.mergeCells(`A1:${lastColLetter}1`);
    const title = ws.getCell('A1');
    title.value = sheetName;
    title.font = { bold: true, size: 18, color: { argb: COLORS.headerBg } };
    ws.getRow(1).height = 28;

    ws.mergeCells(`A2:${lastColLetter}2`);
    const totalMonto = filtered.reduce((s, r) => s + (r.saldo || 0), 0);
    const sub = ws.getCell('A2');
    sub.value = `${filtered.length} facturas · $${totalMonto.toLocaleString('es-CL')}`;
    sub.font = { size: 11, color: { argb: theme.fg } };
    ws.getRow(2).height = 18;

    ws.getRow(3).height = 8; // spacer

    // ======= Tabla =======
    const headerRow = ws.getRow(4);
    headerRow.values = columns.map(c => c.header);
    applyHeaderRow(ws, headerRow);

    const dataStart = 5;
    for (let i = 0; i < filtered.length; i++) {
        const r = filtered[i];
        const data = rowFromData(r, clientsMap, folioMetaMap, factoringMap);
        const row = ws.getRow(dataStart + i);
        row.values = columns.map(c => data[c.key]);
        row.eachCell({ includeEmpty: true }, (cell) => {
            cell.alignment = { vertical: 'middle' };
        });
    }
    const dataEnd = dataStart + filtered.length - 1;

    // Formatos por columna
    columns.forEach((col, idx) => {
        const c = idx + 1;
        const column = ws.getColumn(c);
        column.width = col.width;
        if (col.type === 'money') column.numFmt = '"$"#,##0';
        if (col.type === 'date') column.numFmt = 'dd-mm-yyyy';
        if (col.type === 'number') column.alignment = { horizontal: 'center' };
        if (col.type === 'money') column.alignment = { horizontal: 'right' };
    });

    // Estilo zebra
    applyZebraStripes(ws, dataStart, dataEnd, 1, columns.length);
    applyBorders(ws, dataStart, dataEnd, 1, columns.length);

    // Coloración manual de "Días mora" (más robusto que conditional formatting)
    if (filtered.length > 0 && category === 'Morosa' && diasMoraIdx > 0) {
        for (let i = 0; i < filtered.length; i++) {
            const r = filtered[i];
            const cell = ws.getCell(dataStart + i, diasMoraIdx);
            const d = r.diasMora ?? 0;
            if (d > 90) {
                cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.rose } };
                cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
            } else if (d > 60) {
                cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.roseSoft } };
                cell.font = { color: { argb: COLORS.rose }, bold: true };
            } else if (d > 30) {
                cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.amberSoft } };
                cell.font = { color: { argb: COLORS.amber }, bold: true };
            } else {
                cell.font = { color: { argb: COLORS.zinc700 } };
            }
            cell.alignment = { horizontal: 'center' };
        }
    }

    // Highlight informativo en columnas de factoring cuando el folio fue cedido.
    // No usamos verde "cubierto": el saldo neto del cliente NO desaparece por
    // haber factoring (Defontana ya descontó el anticipo del saldo del cliente,
    // que sigue debiendo el neto).
    if (filtered.length > 0 && anticipoIdx > 0 && factoringMap && factoringMap.size > 0) {
        for (let i = 0; i < filtered.length; i++) {
            const r = filtered[i];
            const fact = factoringMap.get(`${r.docType}:${r.folio}`);
            if (!fact || !fact.totalCedido) continue;
            const cell = ws.getCell(dataStart + i, anticipoIdx);
            cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.indigoSoft } };
            cell.font = { bold: true, color: { argb: COLORS.indigo } };
            if (empresaIdx > 0) {
                const empresaCell = ws.getCell(dataStart + i, empresaIdx);
                empresaCell.font = { color: { argb: COLORS.indigo }, bold: true };
            }
        }
    }

    // Auto-filter
    if (filtered.length > 0) {
        ws.autoFilter = {
            from: { row: 4, column: 1 },
            to: { row: 4, column: columns.length }
        };
    }
}

// Hoja "Todo detallado" con columna Categoría al principio
function buildAllDetailSheet(wb, rows, clientsMap, folioMetaMap, factoringMap, columns) {
    const ws = wb.addWorksheet('Todo detallado', {
        views: [{ state: 'frozen', ySplit: 1, showGridLines: false }]
    });

    const cols = [{ key: 'categoria', header: 'Categoría', width: 22, type: 'text' }, ...columns];
    const headerRow = ws.getRow(1);
    headerRow.values = cols.map(c => c.header);
    applyHeaderRow(ws, headerRow);

    for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const data = rowFromData(r, clientsMap, folioMetaMap, factoringMap);
        data.categoria = classifyCategory(r);
        const row = ws.getRow(2 + i);
        row.values = cols.map(c => data[c.key]);

        // Cell coloring de categoría
        const theme = CATEGORY_THEME[data.categoria];
        if (theme) {
            const cell = row.getCell(1);
            cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: theme.accent } };
            cell.font = { color: { argb: theme.fg }, bold: true, size: 10 };
        }
    }
    const dataEnd = 1 + rows.length;

    cols.forEach((col, idx) => {
        const c = idx + 1;
        const column = ws.getColumn(c);
        column.width = col.width;
        if (col.type === 'money') column.numFmt = '"$"#,##0';
        if (col.type === 'date') column.numFmt = 'dd-mm-yyyy';
        if (col.type === 'number') column.alignment = { horizontal: 'center' };
        if (col.type === 'money') column.alignment = { horizontal: 'right' };
    });

    applyBorders(ws, 2, dataEnd, 1, cols.length);

    ws.autoFilter = {
        from: { row: 1, column: 1 },
        to: { row: 1, column: cols.length }
    };
}

// ============ ENTRY POINT ============

async function buildClientsMap() {
    const all = await listClients();
    const map = new Map();
    for (const c of all) {
        const rut = c.legalCode || c.fileID;
        if (rut) map.set(rut, c);
    }
    return map;
}

export async function generateMorosasExcel({ includeAll = true, withFactoring = true } = {}) {
    const snap = await getLatestSnapshot({ type: 'morosas' });
    if (!snap) {
        throw new Error('No hay snapshot disponible. Generá primero un reporte de cobranza.');
    }

    const rows = snap.rows || [];
    const pairs = rows.map(r => ({ folio: r.folio, docType: r.docType }));

    const columns = buildColumns(withFactoring);

    const baseFetches = [
        getFolioMetaMap(pairs),
        buildClientsMap()
    ];
    const [folioMetaMap, clientsMap, factoringMap] = await Promise.all([
        ...baseFetches,
        withFactoring
            ? getFactoringMap(pairs).catch(err => {
                console.warn('[excel] factoring map fallback (vacío):', err.message);
                return new Map();
            })
            : Promise.resolve(new Map())
    ]);

    const wb = new ExcelJS.Workbook();
    wb.creator = 'Olimpia Cobranza';
    wb.created = new Date();
    wb.properties.date1904 = false;

    // Trabajamos solo con lo realmente cobrable: morosas + vence hoy.
    // Pendientes (vencimiento futuro) y "créditos a favor" (apuntes contables
    // negativos sin sustento real) se descartan para evitar inflar el reporte.
    const cobrables = rows.filter(r => r.esMoroso || r.venceHoy);

    // 1) Morosa
    buildCategorySheet(wb, 'Morosa', cobrables, 'Morosa', clientsMap, folioMetaMap, factoringMap, columns);

    // 2) Resumen ejecutivo
    buildResumenSheet(wb, snap, cobrables);

    // 3) Vence hoy
    buildCategorySheet(wb, 'Vence hoy', cobrables, 'Vence hoy', clientsMap, folioMetaMap, factoringMap, columns);

    // 4) Todo detallado (solo lo cobrable, con tag de categoría)
    buildAllDetailSheet(wb, cobrables, clientsMap, folioMetaMap, factoringMap, columns);

    const suffix = withFactoring ? '' : '-sin-factoring';
    const buffer = await wb.xlsx.writeBuffer();
    return {
        buffer: Buffer.from(buffer),
        filename: `cobranza${suffix}-${new Date().toISOString().substring(0, 10)}.xlsx`,
        rowCount: rows.length,
        generatedAt: snap.generatedAt
    };
}
