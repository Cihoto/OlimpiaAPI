// Orquesta la generación del reporte de cuentas por cobrar morosas para Olimpia.
// Solo usa endpoints GET. No modifica nada en Defontana.

import {
    getAllClients,
    getDocumentsToPay,
    getClientCredit,
    getSaleByFolio
} from './accountingDefontanaService.js';
import { getDeadFolioSet } from './mongoDeadFolios.js';
import { isExcludedRut, getCachedClients, getClientsOrSync } from './clientsSyncService.js';
import { getFolioMetaMap } from './mongoFolioMeta.js';

const SALE_BASE = (process.env.SALE_API_URL || 'https://api.defontana.com/api/Sale/').replace(/\/+$/, '/');

async function getClientByRut(apiKey, rut) {
    const url = `${SALE_BASE}GetClientsByFileID?fileId=${encodeURIComponent(rut)}&status=1&itemsPerPage=1&pageNumber=1`;
    const r = await fetch(url, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` }
    });
    const body = await r.json();
    return body?.clientList?.[0] || null;
}

function daysBetween(fromIso, toIso) {
    if (!fromIso || !toIso) return null;
    const from = new Date(fromIso);
    const to = new Date(toIso);
    if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) return null;
    return Math.floor((to.getTime() - from.getTime()) / (1000 * 60 * 60 * 24));
}

function bucketize(diasMora) {
    if (diasMora == null) return 'sin_fecha';
    if (diasMora <= 0) return 'no_moroso';  // 0 = vence hoy, va a sección aparte
    if (diasMora <= 30) return '1-30';
    if (diasMora <= 60) return '31-60';
    if (diasMora <= 90) return '61-90';
    return '90+';
}

// Defontana representa cada folio como N filas (factura + apuntes contables).
// REGLA (validada contra CSV de Antonio en MOROSAS):
//   - Agrupamos por (docType, folio).
//   - Si netSum ≈ 0  → folio totalmente compensado, descartar todas las líneas.
//   - Si netSum < 0  → cliente tiene saldo a favor o pago duplicado en ese folio.
//                       Descartar todas las líneas (el cliente NO debe nada).
//   - Si netSum > 0  → emitimos cada línea como row independiente. La lógica
//                       posterior decide morosa/vence hoy/pendiente/crédito.
function mapDocumentsToRows(documents) {
    const byFolio = new Map();
    for (const d of documents) {
        const key = `${d.idTipoDocumento || ''}:${d.number}`;
        if (!byFolio.has(key)) byFolio.set(key, []);
        byFolio.get(key).push(d);
    }

    const rows = [];
    for (const [, lines] of byFolio) {
        const netSum = lines.reduce((s, l) => s + (l.amount || 0), 0);
        if (netSum < 1) continue; // saldado (=0) o cliente a favor (<0) → fuera
        for (const d of lines) {
            if (Math.abs(d.amount) < 1) continue;
            rows.push({
                folio: d.number,
                docType: d.idTipoDocumento,
                documentTypeName: d.documentType,
                codeSii: d.codelect,
                expirationDate: d.expirationDate,
                amount: d.amount || 0
            });
        }
    }
    return rows;
}

// Construye una fila del reporte para Vista 1.
// CONVENCIÓN DEFONTANA (verificada contra GetClientCredit):
//   amount > 0  →  deuda del cliente (factura/cargo por cobrar)
//   amount < 0  →  crédito a favor del cliente (NC sin aplicar / anticipo)
//   GetClientCredit.saldoPendiente positivo = cliente debe; negativo = cliente tiene saldo a favor.
function buildRow({ folio, docType, documentTypeName, codeSii, expirationDate, amount }, client, today) {
    const saldo = Math.abs(amount);
    const diasMora = daysBetween(expirationDate, today);
    const esFactura = amount > 0;
    const esCreditoCliente = amount < 0;
    return {
        folio,
        docType,
        documentTypeName,
        codeSii,
        rut: client.legalCode || client.fileID,
        // name = razón social (CENCOSUD RETAIL S.A.); business = giro (VENTA AL POR MENOR)
        clientName: client.name || client.business || null,
        giro: client.business || null,
        sellerFileId: client.sellerID || client.salesmanID || null,
        condicionPago: client.paymentID || null,
        diasPromedioPago: client.daysAvgPayment || client.diasPromedioPago || null,
        fechaVencimiento: expirationDate,
        saldo,
        amountRaw: amount,
        esFactura,
        esCreditoCliente,
        diasMora,
        bucket: bucketize(diasMora),
        // Moroso = factura por cobrar Y estrictamente vencida (diasMora > 0).
        // Las que vencen HOY (diasMora === 0) NO son morosas — se muestran aparte
        // como "pagos esperados pendientes" porque el cliente aún tiene el día.
        esMoroso: esFactura && diasMora != null && diasMora > 0,
        venceHoy: esFactura && diasMora === 0
    };
}

/**
 * Genera el reporte de morosas combinando:
 *   1. Lista de clientes activos (GetClients)
 *   2. Documentos pendientes de pago por cliente (GetDocumentsToPay) -> Accounting
 *   3. Cruce con dead_folios (folios muertos) para descartar fantasmas
 *
 * @param {string} apiKey - Bearer de Defontana
 * @param {object} opts
 * @param {number} [opts.maxClients] - limitar para demo (ej. 10). Sin valor = todos.
 * @param {boolean} [opts.includeDead=false] - si true, no filtra muertos (debug).
 * @param {string[]} [opts.rutFilter] - si se entrega, solo procesa estos RUTs.
 * @returns {Promise<{ rows, summary, generatedAt }>}
 */
export async function generateMorosasReport(apiKey, opts = {}) {
    const { maxClients, includeDead = false, rutFilter } = opts;
    const today = new Date().toISOString();

    const deadSet = includeDead ? new Set() : await getDeadFolioSet();

    // 1. Lista de clientes
    let clients;
    if (rutFilter && rutFilter.length > 0) {
        // Filtrar RUTs excluidos siempre
        const requested = rutFilter.filter(r => !isExcludedRut(r));
        // Enriquecer cada RUT con datos del cliente para tener name/vendedor
        clients = await Promise.all(requested.map(async (rut) => {
            try {
                const data = await getClientByRut(apiKey, rut);
                return data || { legalCode: rut, fileID: rut, name: null };
            } catch {
                return { legalCode: rut, fileID: rut, name: null };
            }
        }));
    } else {
        // Usa cache Mongo (mucho más rápido). Si está vacío, sincroniza primero.
        clients = await getClientsOrSync(apiKey);
    }

    // Defensa extra: filtrar excluidos siempre
    clients = clients.filter(c => !isExcludedRut(c.legalCode || c.fileID));

    if (maxClients) clients = clients.slice(0, maxClients);

    const rows = [];
    const errors = [];
    let processed = 0;

    for (const client of clients) {
        const rut = client.legalCode || client.fileID;
        if (!rut) continue;

        try {
            const { success, documents = [] } = await getDocumentsToPay(apiKey, rut);
            if (!success) continue;

            const mapped = mapDocumentsToRows(documents);
            for (const doc of mapped) {
                if (Math.abs(doc.amount) < 1) continue; // saldo ~0, ya pagado
                const key = `${doc.docType || ''}:${doc.folio}`;
                if (deadSet.has(key)) continue; // folio muerto, descartar
                rows.push(buildRow(doc, client, today));
            }
        } catch (err) {
            errors.push({ rut, error: err.message });
        }
        processed += 1;
    }

    // Enriquecer con cache de folios_meta (emissionDate, totalOriginal) si está disponible
    if (rows.length > 0) {
        const pairs = rows.map(r => ({ folio: r.folio, docType: r.docType }));
        const metaMap = await getFolioMetaMap(pairs);
        for (const r of rows) {
            const m = metaMap.get(`${r.docType}:${r.folio}`);
            if (m) {
                r.emissionDate = m.emissionDate || null;
                r.totalOriginal = m.total ?? null;
                // si el sellerFileId no vino del cliente (raro), usar el del folio
                if (!r.sellerFileId && m.sellerFileId) r.sellerFileId = m.sellerFileId;
            } else {
                r.emissionDate = null;
                r.totalOriginal = null;
            }
        }
    }

    // Resumen
    // Reglas:
    //   - Moroso = factura por cobrar (amount > 0) Y vencida
    //   - Crédito vencido = NC/anticipo a favor del cliente (amount < 0) cuya fecha ya pasó
    //   - Saldo neto del cliente = morosasTotal - creditosTotal (cuadra con GetClientCredit.saldoPendiente)
    const morosas = rows.filter(r => r.esMoroso); // ya incluye amount > 0 + vencido (estricto)
    const vencenHoy = rows.filter(r => r.venceHoy); // facturas que vencen HOY (no morosas aún)
    const creditosVencidos = rows.filter(r => r.esCreditoCliente && r.diasMora != null && r.diasMora > 0);

    const buckets = { '1-30': 0, '31-60': 0, '61-90': 0, '90+': 0 };
    const totalsByBucket = { '1-30': 0, '31-60': 0, '61-90': 0, '90+': 0 };
    for (const r of morosas) {
        if (buckets[r.bucket] !== undefined) {
            buckets[r.bucket] += 1;
            totalsByBucket[r.bucket] += r.saldo;
        }
    }

    // Totales por cliente: solo morosas vencidas (no resta créditos del cliente, son cosas distintas)
    const totalsByClient = {};
    const totalsByVendedor = {};
    for (const r of morosas) {
        const ck = r.rut || 'sin_rut';
        totalsByClient[ck] = totalsByClient[ck] || { rut: r.rut, name: r.clientName, total: 0, creditos: 0, count: 0 };
        totalsByClient[ck].total += r.saldo;
        totalsByClient[ck].count += 1;
        const vk = r.sellerFileId || 'sin_vendedor';
        totalsByVendedor[vk] = totalsByVendedor[vk] || { vendedor: r.sellerFileId, total: 0, count: 0 };
        totalsByVendedor[vk].total += r.saldo;
        totalsByVendedor[vk].count += 1;
    }
    // Adjuntar créditos vencidos por cliente (informativo)
    for (const r of creditosVencidos) {
        const ck = r.rut || 'sin_rut';
        if (totalsByClient[ck]) {
            totalsByClient[ck].creditos += r.saldo;
        }
    }

    const morosasTotal = morosas.reduce((s, r) => s + r.saldo, 0);
    const creditosTotal = creditosVencidos.reduce((s, r) => s + r.saldo, 0);

    return {
        generatedAt: today,
        rows,
        summary: {
            clientsProcessed: processed,
            clientsTotal: clients.length,
            rowsTotal: rows.length,
            morosasCount: morosas.length,
            morosasTotal,           // solo facturas por cobrar (diasMora > 0)
            vencenHoyCount: vencenHoy.length,
            vencenHoyTotal: vencenHoy.reduce((s, r) => s + r.saldo, 0),
            creditosCount: creditosVencidos.length,
            creditosTotal,          // créditos a favor del cliente
            saldoNeto: morosasTotal - creditosTotal,
            buckets,
            totalsByBucket,
            totalsByClient: Object.values(totalsByClient).sort((a, b) => b.total - a.total),
            totalsByVendedor: Object.values(totalsByVendedor).sort((a, b) => b.total - a.total),
            errors
        }
    };
}
