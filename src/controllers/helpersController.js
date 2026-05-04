import fs from 'fs';
import path from 'path';
import { createHash, randomUUID } from 'crypto';
import { getClientsByRut, getAllClients } from '../services/mongoClientDataService.js';
import OpenAI from 'openai';
import moment from 'moment';
import XLSX from 'xlsx';
import pdfParse from 'pdf-parse';
import findDeliveryDayByComuna, { resolveDispatchCutoffHourByComuna } from '../utils/findDeliveryDate.js'; // Import the function to find delivery day by comuna
import findRappiDeliveryDayByComuna from '../utils/findRappiDeliveryDate.js';
import foundSpecialCustomers from '../services/foundSpecialCustomers.js';
import {
    analyzeOrderEmail,
    analyzeOrderEmailFromGmail,
    extractPedidosYaOrderNumber,
    parsePedidosYaOrderQuantities,
    extractPedidosYaOrdersFromAttachment
} from '../services/analyzeOrderEmail.js'; // Import the function to analyze order email
import { parseKeyLogisticsOrderText, EMPTY_ORDER_QUANTITIES } from '../services/keyLogisticsOrderParser.js';
import { parseRappiTurboOrderText } from '../services/rappiTurboOrderParser.js';
import { createDeliveryReservationForAnalysis } from '../services/deliveryCapacityService.js';
import {
    findProcessedKeyLogisticsOrder,
    insertProcessedKeyLogisticsOrder,
    findProcessedSenderOrder,
    insertProcessedSenderOrder
} from '../services/mongoOrderRegistry.js';
import {
    createManualOcRecord,
    findManualOcRecord,
    updateManualOcRecord,
    appendManualOcTimeline,
    findLatestManualOcByDetectedOrderNumber
} from '../services/mongoManualOcRegistry.js';
import {
    acquireManualOcSubmitLock,
    releaseManualOcSubmitLock
} from '../services/mongoManualOcSubmitLockRegistry.js';
import {
    buildGmailClient,
    decodeBase64Url,
    extractEmailAddress,
    extractEmailText,
    findExcelAttachments,
    findPdfAttachments,
    headersToMap
} from '../utils/Google/gmail.js';
import {
    buildManualOcBatchAnalysis,
    buildManualOcSuccessAnalysis,
    classifyManualOcParserFailure
} from '../utils/manualOcAnalysis.js';

function isManualOcDeveloperFlagEnabled(value) {
    const normalized = String(value || '').trim().toLowerCase();
    return ['1', 'true', 'yes', 'y', 'si', 'sí', 'on'].includes(normalized);
}

function parseEnvBooleanLoose(value, fallback = false) {
    const normalized = String(value || '').trim().toLowerCase();
    if (normalized === '') {
        return fallback;
    }
    if (['1', 'true', 'yes', 'y', 'si', 'sí', 'on'].includes(normalized)) {
        return true;
    }
    if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) {
        return false;
    }
    return fallback;
}

const client = new OpenAI();
const KEY_LOGISTICS_BLOCKED_RUTS = new Set(['77.419.327-8', '96.930.440-6']);
const KEY_LOGISTICS_FIXED_RUT = '96.930.440-6';
const KEY_LOGISTICS_SENDER = 'fax@keylogistics.cl';
const PEDIDOS_YA_SENDER = 'compras.marketds@pedidosya.com';
const RAPPI_TURBO_SENDER = 'tomas.bravo@rappi.com';
const MAKE_MANUAL_OC_WEBHOOK_URL = process.env.MAKE_MANUAL_OC_WEBHOOK_URL || '';
const MANUAL_OC_MAKE_MODE_DEFAULT = String(process.env.MANUAL_OC_MAKE_MODE || 'TEST_ONLY').trim() || 'TEST_ONLY';
const MANUAL_OC_MAKE_TEST_MODE_DEFAULT = parseEnvBooleanLoose(process.env.MANUAL_OC_MAKE_TEST_MODE, true);
const MANUAL_OC_MAKE_PREVENT_BILLING_DEFAULT = parseEnvBooleanLoose(
    process.env.MANUAL_OC_MAKE_PREVENT_BILLING,
    true
);
const MANUAL_OC_DEVELOPER_MODE_DEFAULT = isManualOcDeveloperFlagEnabled(
    process.env.MANUAL_OC_DEVELOPER_MODE || process.env.MANUAL_OC_DEV_MODE || ''
);
const MANUAL_OC_DEVELOPER_OUTBOX_DIR = process.env.MANUAL_OC_DEVELOPER_OUTBOX_DIR
    || path.resolve(process.cwd(), 'temp', 'manual_oc_developer_outbox');

const MANUAL_OC_CLIENT_PROFILES = Object.freeze({
    PEYA: Object.freeze({
        sourceClientCode: 'PEYA',
        sourceClientName: 'PedidosYa',
        syntheticSender: PEDIDOS_YA_SENDER,
        parserProfile: 'pedidos_ya_excel_v1'
    })
});

const MANUAL_OC_DEFAULT_GIRO = 'COMERCIALIZADORA AL POR MAYOR Y MENOR DE ALIMENTOS';
const MANUAL_OC_DEFAULT_STORAGE = 'BODEGACENTRAL';
const MANUAL_OC_DEFAULT_SHOP_ID = 'Local';
const MANUAL_OC_DEFAULT_PRICE_LIST = '1';
const MANUAL_OC_DEFAULT_DOCUMENT_TYPE = 'FVAELECT';
const MANUAL_OC_DISPATCH_CUTOFF_HOUR = Number.parseInt(process.env.MANUAL_OC_DISPATCH_CUTOFF_HOUR || '12', 10);
const MANUAL_OC_PROCESSED_STATUSES = Object.freeze([
    'submit_processing',
    'submitted_to_make',
    'submit_skipped_make'
]);
const ADDRESS_MATCH_MIN_CONFIDENCE = (() => {
    const rawValue = Number.parseInt(process.env.ADDRESS_MATCH_MIN_CONFIDENCE || '75', 10);
    if (!Number.isFinite(rawValue)) {
        return 75;
    }
    return Math.max(0, Math.min(100, rawValue));
})();
const ADDRESS_MATCH_MIN_SCORE = (() => {
    const rawValue = Number.parseFloat(process.env.ADDRESS_MATCH_MIN_SCORE || '0.75');
    if (!Number.isFinite(rawValue)) {
        return 0.75;
    }
    return Math.max(0, Math.min(1, rawValue));
})();
const ADDRESS_MATCH_GPT_MODELS = (() => {
    const configuredModels = String(
        process.env.ADDRESS_MATCH_GPT_MODELS
        || process.env.GPT_ADDRESS_MATCH_MODELS
        || 'gpt-5,gpt-5-mini'
    )
        .split(',')
        .map((value) => String(value || '').trim())
        .filter(Boolean);
    return configuredModels.length > 0 ? configuredModels : ['gpt-5-mini'];
})();
const ADDRESS_MATCH_LLM_FALLBACK_MIN_SCORE = (() => {
    const rawValue = Number.parseFloat(process.env.ADDRESS_MATCH_LLM_FALLBACK_MIN_SCORE || '0.55');
    if (!Number.isFinite(rawValue)) {
        return 0.55;
    }
    return Math.max(0, Math.min(1, rawValue));
})();
const ADDRESS_MATCH_LLM_FALLBACK_TOPN = (() => {
    const rawValue = Number.parseInt(process.env.ADDRESS_MATCH_LLM_FALLBACK_TOPN || '5', 10);
    if (!Number.isFinite(rawValue) || rawValue <= 0) {
        return 5;
    }
    return Math.min(20, rawValue);
})();

const MANUAL_OC_DETAIL_MAPPING = Object.freeze([
    Object.freeze({ quantityKey: 'Pedido_Cantidad_Amargo', code: '17798147780069', priceBucket: '150' }),
    Object.freeze({ quantityKey: 'Pedido_Cantidad_Leche', code: '17798147780052', priceBucket: '150' }),
    Object.freeze({ quantityKey: 'Pedido_Cantidad_Pink', code: '70724043633542', priceBucket: '150' }),
    Object.freeze({ quantityKey: 'Pedido_Cantidad_Pink_90g', code: '70724043633549', priceBucket: '90' }),
    Object.freeze({ quantityKey: 'Pedido_Cantidad_Leche_90g', code: '70724043633550', priceBucket: '90' }),
    Object.freeze({ quantityKey: 'Pedido_Cantidad_Free', code: '70724043633551', priceBucket: 'free' })
]);

function normalizeManualOcOrderNumber(value) {
    const normalized = String(value || '')
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '')
        .trim();

    if (!normalized) {
        return null;
    }

    const withPrefix = normalized.startsWith('PO') ? normalized : `PO${normalized}`;
    return /^PO\d{5,}$/.test(withPrefix) ? withPrefix : null;
}

function extractManualOcOrderNumber({ fileName, text }) {
    const extracted = extractPedidosYaOrderNumber({
        attachmentFilename: fileName,
        emailAttached: text,
        emailSubject: '',
        emailBody: ''
    });

    return normalizeManualOcOrderNumber(extracted);
}

function normalizeSkuLikeValue(value) {
    return String(value || '')
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '')
        .trim();
}

function parsePeyaPdfItems(text) {
    const lines = String(text || '')
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);
    const items = [];

    for (const rawLine of lines) {
        const compactLine = rawLine.replace(/\s+/g, '');
        const rowMatch = compactLine.match(/^([A-Z0-9]{6})(\d{12,14})(.+)$/);
        if (!rowMatch) {
            continue;
        }

        const sku = rowMatch[1];
        const ean = rowMatch[2];
        const rowTail = rowMatch[3];
        const unitsChunkMatch = rowTail.match(/(\d{2,7})(?=\d{1,3}(?:[.,]\d{3})+[.,]\d{2}\$?)/);
        if (!unitsChunkMatch) {
            continue;
        }

        const digits = String(unitsChunkMatch[1] || '').replace(/\D/g, '');
        let quantityBoxes = 0;
        let quantityUnits = 0;
        if (digits.length >= 2) {
            const maxBoxDigits = Math.min(3, digits.length - 1);
            for (let boxDigits = 1; boxDigits <= maxBoxDigits; boxDigits += 1) {
                const boxes = Number(digits.slice(0, boxDigits));
                const units = Number(digits.slice(boxDigits));
                if (!Number.isFinite(boxes) || !Number.isFinite(units) || boxes <= 0 || units <= 0) {
                    continue;
                }
                if (units % 24 === 0 && (units / 24) === boxes) {
                    quantityBoxes = boxes;
                    quantityUnits = units;
                    break;
                }
            }
        } else if (digits.length > 0) {
            quantityBoxes = Number(digits);
            quantityUnits = quantityBoxes > 0 ? quantityBoxes * 24 : 0;
        }

        const moneyMatches = [...rowTail.matchAll(/(\d{1,3}(?:[.,]\d{3})+[.,]\d{2})\$/g)].map((match) => match[1]);
        let unitCost = parseNumberish(moneyMatches[0] || null);
        const costExclIva = parseNumberish(moneyMatches[1] || null);
        const costIncIva = parseNumberish(moneyMatches[2] || null);

        if (quantityUnits > 0 && costExclIva > 0 && (unitCost <= 0 || unitCost > costExclIva)) {
            unitCost = Math.round((costExclIva / quantityUnits) * 1000) / 1000;
        }

        const description = rowTail
            .slice(0, unitsChunkMatch.index)
            .replace(/\s+/g, ' ')
            .trim() || null;

        items.push({
            sku: sku || null,
            ean: ean || null,
            description,
            quantityBoxes: quantityBoxes || null,
            quantityUnits: quantityUnits || null,
            unitCost: unitCost > 0 ? unitCost : null,
            costExclIva: costExclIva > 0 ? costExclIva : null,
            costIncIva: costIncIva > 0 ? costIncIva : null
        });
    }

    return items;
}

function extractPeyaPdfAnalysis(text) {
    const sourceText = String(text || '');
    if (!sourceText.trim()) {
        return null;
    }

    const pick = (regex) => {
        const match = sourceText.match(regex);
        return match && match[1] ? String(match[1]).replace(/\s+/g, ' ').trim() : null;
    };

    const purchaseOrderNumber = normalizeManualOcOrderNumber(
        pick(/\b(PO\d{5,})\b/i)
    );

    const items = parsePeyaPdfItems(sourceText);
    const quantityBoxesTotal = items.reduce((acc, item) => acc + (item.quantityBoxes || 0), 0);
    const quantityUnitsTotal = items.reduce((acc, item) => acc + (item.quantityUnits || 0), 0);
    const costExclIvaTotal = items.reduce((acc, item) => acc + (item.costExclIva || 0), 0);
    const costIncIvaTotal = items.reduce((acc, item) => acc + (item.costIncIva || 0), 0);

    return {
        pattern: 'peya_pdf_oc_v1',
        purchaseOrderNumber,
        metadata: {
            store: pick(/Store:\s*([\s\S]*?)(?=Proveedor:|\n|\t|$)/i),
            proveedor: pick(/Proveedor:\s*([\s\S]*?)(?=Razon Social:|\n|\t|$)/i),
            razonSocial: pick(/Razon Social:\s*([\s\S]*?)(?=RUT:|\n|\t|$)/i),
            rut: pick(/RUT:\s*([0-9.\-kK]+)/i),
            direccionEntrega: pick(/Direccion entrega:\s*([\s\S]*?)(?=Fecha entrega:|\n|\t|$)/i),
            provincia: pick(/Provincia:\s*([\s\S]*?)(?=Fecha venc\. PO:|\n|\t|$)/i),
            pais: pick(/Pais:\s*([\s\S]*?)(?=Contacto encargado tienda:|\n|\t|$)/i),
            horarioRecepcion: pick(/Horario\s*Recepcion:\s*([\s\S]*?)(?=P Market|\n{2,}|$)/i),
            contactoEncargadoTienda: pick(/Contacto encargado tienda:\s*([\s\S]*?)(?=Costo total Inc\. IVA|\n|\t|$)/i)
        },
        dates: {
            fechaEmision: parseManualDateCandidate(pick(/Fecha emision:\s*([\s\S]*?)(?=Costo total Excl\. IVA|\n|\t|$)/i)),
            fechaEntrega: parseManualDateCandidate(pick(/Fecha entrega:\s*([\s\S]*?)(?=Provincia:|\n|\t|$)/i)),
            fechaVencimientoPo: parseManualDateCandidate(pick(/Fecha venc\. PO:\s*([\s\S]*?)(?=IVA|\n|\t|$)/i))
        },
        totals: {
            costoTotalExclIva: parseNumberish(pick(/Costo total Excl\. IVA\s*([0-9.,]+)\$?/i)),
            ivaTotal: parseNumberish(pick(/Fecha venc\. PO:[\s\S]*?IVA\s*([0-9.,]+)\$?/i)),
            costoTotalIncIva: parseNumberish(pick(/Costo total Inc\. IVA\s*([0-9.,]+)\$?/i))
        },
        items,
        itemStats: {
            count: items.length,
            quantityBoxesTotal: Math.round(quantityBoxesTotal * 1000) / 1000,
            quantityUnitsTotal: Math.round(quantityUnitsTotal * 1000) / 1000,
            costExclIvaTotal: Math.round(costExclIvaTotal * 1000) / 1000,
            costIncIvaTotal: Math.round(costIncIvaTotal * 1000) / 1000
        }
    };
}

function buildManualOcPayloadForQuantities({ fileName, mimeType, text }) {
    return JSON.stringify({
        attachmentFilename: String(fileName || ''),
        mimeType: String(mimeType || ''),
        emailAttached: String(text || ''),
        emailBody: '',
        emailSubject: ''
    });
}

function buildManualOcAttachmentPayload({ fileName, mimeType, text, targetOrderNumber = null }) {
    return {
        attachmentFilename: String(fileName || ''),
        mimeType: String(mimeType || ''),
        emailAttached: String(text || ''),
        emailBody: '',
        emailSubject: '',
        targetOrderNumber: targetOrderNumber || null,
        manualOc: {
            targetOrderNumber: targetOrderNumber || null
        }
    };
}

function buildManualOcSyntheticAnalysisFromExtractedOrder(order) {
    const safeOrder = order || {};
    return {
        pattern: safeOrder.sourceFormat || null,
        purchaseOrderNumber: normalizeManualOcOrderNumber(safeOrder.orderNumber),
        metadata: {
            store: safeOrder.storeName || null,
            direccionEntrega: safeOrder.storeAddress || null
        },
        dates: {
            fechaEmision: parseManualDateCandidate(safeOrder.orderDate),
            fechaEntrega: parseManualDateCandidate(safeOrder.deliveryDate)
        },
        itemStats: {
            count: toPositiveInteger(safeOrder.itemCount),
            quantityBoxesTotal: toPositiveInteger(
                (safeOrder.quantities?.Pedido_Cantidad_Pink || 0)
                + (safeOrder.quantities?.Pedido_Cantidad_Amargo || 0)
                + (safeOrder.quantities?.Pedido_Cantidad_Leche || 0)
                + (safeOrder.quantities?.Pedido_Cantidad_Free || 0)
                + (safeOrder.quantities?.Pedido_Cantidad_Pink_90g || 0)
                + (safeOrder.quantities?.Pedido_Cantidad_Amargo_90g || 0)
                + (safeOrder.quantities?.Pedido_Cantidad_Leche_90g || 0)
            ),
            quantityUnitsTotal: 0
        },
        items: []
    };
}

function buildManualOcComparableSnapshot({
    detectedOrderNumber,
    quantities,
    excelAnalysis,
    pdfAnalysis
}) {
    const analysis = excelAnalysis || pdfAnalysis || {};
    const totals = analysis?.totals || {};
    const items = Array.isArray(analysis?.items) ? analysis.items : [];

    const normalizedItems = items
        .map((item) => ({
            sku: normalizeSkuLikeValue(item?.sku),
            ean: (() => {
                const digits = String(item?.ean || '').replace(/\D/g, '');
                if (!digits) {
                    return normalizeSkuLikeValue(item?.ean);
                }
                const withoutLeadingZeros = digits.replace(/^0+/, '');
                return withoutLeadingZeros || digits;
            })(),
            quantityBoxes: toPositiveInteger(item?.quantityBoxes),
            quantityUnits: toPositiveInteger(item?.quantityUnits),
            costExclIva: Math.round(parseNumberish(item?.costExclIva || 0)),
            costIncIva: Math.round(parseNumberish(item?.costIncIva || 0))
        }))
        .filter((item) => item.sku || item.ean || item.quantityBoxes || item.quantityUnits)
        .sort((a, b) => {
            const aKey = `${a.sku}|${a.ean}`;
            const bKey = `${b.sku}|${b.ean}`;
            return aKey.localeCompare(bKey);
        });

    return {
        detectedOrderNumber: normalizeManualOcOrderNumber(detectedOrderNumber),
        quantities: {
            Pedido_Cantidad_Pink: toPositiveInteger(quantities?.Pedido_Cantidad_Pink),
            Pedido_Cantidad_Amargo: toPositiveInteger(quantities?.Pedido_Cantidad_Amargo),
            Pedido_Cantidad_Leche: toPositiveInteger(quantities?.Pedido_Cantidad_Leche),
            Pedido_Cantidad_Free: toPositiveInteger(quantities?.Pedido_Cantidad_Free),
            Pedido_Cantidad_Pink_90g: toPositiveInteger(quantities?.Pedido_Cantidad_Pink_90g),
            Pedido_Cantidad_Amargo_90g: toPositiveInteger(quantities?.Pedido_Cantidad_Amargo_90g),
            Pedido_Cantidad_Leche_90g: toPositiveInteger(quantities?.Pedido_Cantidad_Leche_90g)
        },
        totals: {
            costoTotalExclIva: Math.round(parseNumberish(totals?.costoTotalExclIva || 0)),
            ivaTotal: Math.round(parseNumberish(totals?.ivaTotal || 0)),
            costoTotalIncIva: Math.round(parseNumberish(totals?.costoTotalIncIva || 0))
        },
        itemStats: {
            count: toPositiveInteger(analysis?.itemStats?.count),
            quantityBoxesTotal: Math.round(parseNumberish(analysis?.itemStats?.quantityBoxesTotal || 0)),
            quantityUnitsTotal: Math.round(parseNumberish(analysis?.itemStats?.quantityUnitsTotal || 0))
        },
        items: normalizedItems
    };
}

function buildManualOcConflictSignature(snapshot) {
    const safeSnapshot = snapshot || {};
    const safeQuantities = safeSnapshot.quantities || {};
    const safeTotals = safeSnapshot.totals || {};
    const safeItems = Array.isArray(safeSnapshot.items) ? safeSnapshot.items : [];

    return {
        detectedOrderNumber: safeSnapshot.detectedOrderNumber || null,
        quantities: {
            Pedido_Cantidad_Pink: toPositiveInteger(safeQuantities.Pedido_Cantidad_Pink),
            Pedido_Cantidad_Amargo: toPositiveInteger(safeQuantities.Pedido_Cantidad_Amargo),
            Pedido_Cantidad_Leche: toPositiveInteger(safeQuantities.Pedido_Cantidad_Leche),
            Pedido_Cantidad_Free: toPositiveInteger(safeQuantities.Pedido_Cantidad_Free),
            Pedido_Cantidad_Pink_90g: toPositiveInteger(safeQuantities.Pedido_Cantidad_Pink_90g),
            Pedido_Cantidad_Amargo_90g: toPositiveInteger(safeQuantities.Pedido_Cantidad_Amargo_90g),
            Pedido_Cantidad_Leche_90g: toPositiveInteger(safeQuantities.Pedido_Cantidad_Leche_90g)
        },
        totals: {
            costoTotalExclIva: Math.round(parseNumberish(safeTotals.costoTotalExclIva || 0)),
            costoTotalIncIva: Math.round(parseNumberish(safeTotals.costoTotalIncIva || 0))
        },
        items: safeItems.map((item) => ({
            sku: normalizeSkuLikeValue(item?.sku),
            quantityBoxes: toPositiveInteger(item?.quantityBoxes),
            quantityUnits: toPositiveInteger(item?.quantityUnits)
        }))
    };
}

function areManualOcSnapshotsEquivalent(left, right) {
    if (!left || !right) {
        return false;
    }
    const leftSignature = buildManualOcConflictSignature(left);
    const rightSignature = buildManualOcConflictSignature(right);
    return JSON.stringify(leftSignature) === JSON.stringify(rightSignature);
}

/**
 * KeyLogistics master data for billing/shipping resolution.
 *
 * Scope:
 * - Applies only to sender `fax@keylogistics.cl` when `keyLogistics.clientId` is recognized.
 * - Does not affect other senders/flows.
 *
 * Note:
 * - `adm_ventas` maps to Pronto Copec.
 */
const KEY_LOGISTICS_CLIENT_MASTER_DATA = Object.freeze({
    enex: Object.freeze({
        'COMPANY NAME ': 'Key Logistics',
        NAME: 'Key Logistics (ENEX)',
        'RAZ\u00d3N SOCIAL': 'KEYLOGISTICS CHILE S A',
        RUT: KEY_LOGISTICS_FIXED_RUT,
        'Direccion Facturación': 'Pio XI 1290',
        'Direccion Despacho': 'av lo espejo 01740, Bodega 3',
        'Comuna Despacho': 'San Bernardo',
        'Region Despacho': 'Santiago',
        'Horario Despacho': '08:00 - 13:00',
        'Precio Caja': 74160,
        diasCredito: '30',
        'Orden de compra (SI O NO)': 'Revisar OC',
        'Unidad/Caja': 'CAJA',
        'EMAIL PEDIDO': '',
        'EMAIL FACTURA': 'jhonlly.sulbaran@keylogistics.cl',
        'CENTRO DE NEGOCIOS ': 'VENTAS',
        'VENDEDOR ': 'FNASSAR',
        BANEADO: 'BAN',
        'Precio Caja 90': 41220,
        'Precio Caja Free': 0
    }),
    esmax: Object.freeze({
        'COMPANY NAME ': 'Key Logistics',
        NAME: 'Key Logistics (ESMAX)',
        'RAZ\u00d3N SOCIAL': 'KEYLOGISTICS CHILE S A',
        RUT: KEY_LOGISTICS_FIXED_RUT,
        'Direccion Facturación': 'Pio XI 1290',
        'Direccion Despacho': 'av lo espejo 01740, Bodega 3',
        'Comuna Despacho': 'San Bernardo',
        'Region Despacho': 'Santiago',
        'Horario Despacho': '08:00 - 13:00',
        'Precio Caja': 79200,
        diasCredito: '30',
        'Orden de compra (SI O NO)': 'Revisar OC',
        'Unidad/Caja': 'CAJA',
        'EMAIL PEDIDO': '',
        'EMAIL FACTURA': 'jhonlly.sulbaran@keylogistics.cl',
        'CENTRO DE NEGOCIOS ': 'VENTAS',
        'VENDEDOR ': 'FNASSAR',
        BANEADO: 'BAN',
        'Precio Caja 90': 41400,
        'Precio Caja Free': 86400
    }),
    oxxo: Object.freeze({
        'COMPANY NAME ': 'Key Logistics',
        NAME: 'Key Logistics (OXXO)',
        'RAZ\u00d3N SOCIAL': 'KEYLOGISTICS CHILE S A',
        RUT: KEY_LOGISTICS_FIXED_RUT,
        'Direccion Facturación': 'Pio XI 1290',
        'Direccion Despacho': 'lago riñihue 2319',
        'Comuna Despacho': 'San Bernardo',
        'Region Despacho': 'Santiago',
        'Horario Despacho': '08:00 - 13:00',
        'Precio Caja': 84480,
        diasCredito: '30',
        'Orden de compra (SI O NO)': 'Revisar OC',
        'Unidad/Caja': 'UNIDAD',
        'EMAIL PEDIDO': '',
        'EMAIL FACTURA': 'jhonlly.sulbaran@keylogistics.cl',
        'CENTRO DE NEGOCIOS ': 'VENTAS',
        'VENDEDOR ': 'FNASSAR',
        BANEADO: 'BAN',
        'Precio Caja 90': 41220,
        'Precio Caja Free': 92928
    }),
    adm_ventas: Object.freeze({
        'COMPANY NAME ': 'Key Logistics',
        NAME: 'Key Logistics (Pronto Copec)',
        'RAZ\u00d3N SOCIAL': 'KEYLOGISTICS CHILE S A',
        RUT: KEY_LOGISTICS_FIXED_RUT,
        'Direccion Facturación': 'Avenida Lo Espejo 01740, Bodega 3, San Bernardo',
        'Direccion Despacho': 'Avenida Lo Espejo 01740, Bodega 3',
        'Comuna Despacho': 'San Bernardo',
        'Region Despacho': 'Santiago',
        'Horario Despacho': '08:00 - 13:00',
        'Precio Caja': 90960,
        diasCredito: '15',
        'Orden de compra (SI O NO)': 'Revisar OC',
        'Unidad/Caja': 'CAJA',
        'EMAIL PEDIDO': '',
        'EMAIL FACTURA': 'jhonlly.sulbaran@keylogistics.cl',
        'CENTRO DE NEGOCIOS ': 'VENTAS',
        'VENDEDOR ': 'FNASSAR',
        BANEADO: 'BAN',
        'Precio Caja 90': 41220,
        'Precio Caja Free': 0
    })
});

function buildKeyLogisticsClientData(clientId, emailDate, extractedBoxPrice) {
    const profile = KEY_LOGISTICS_CLIENT_MASTER_DATA[clientId];
    if (!profile) {
        return null;
    }

    const data = { ...profile };
    const deliveryDay = findDeliveryDayByComuna(
        data['Comuna Despacho'],
        emailDate,
        getRegionFromClientRecord(data)
    );
    if (deliveryDay != null) {
        data.deliveryDay = `${deliveryDay}`;
    } else if (String(data['Direccion Despacho'] || '').toLowerCase() === 'retiro') {
        data.deliveryDay = moment().add(1, 'days').format('YYYY-MM-DD');
    } else {
        data.deliveryDay = '';
    }

    return {
        data,
        length: 1,
        address: true,
        message: 'Cliente KeyLogistics resuelto por matriz fija',
        boxPriceIsEqual: extractedBoxPrice == data['Precio Caja'],
        source: 'keylogistics_master_data'
    };
}

async function reserveRmDeliveryCapacity({ emailData, clientData, emailContext }) {
    let reservationResult = null;
    try {
        reservationResult = await createDeliveryReservationForAnalysis({
            emailData,
            clientData,
            emailContext
        });
    } catch (error) {
        throw {
            code: 500,
            error: 'Error en reserva de despacho',
            message: error?.message || 'No se pudo reservar capacidad de despacho'
        };
    }

    if (!reservationResult || reservationResult.skipped || !reservationResult.deliveryReservation) {
        return null;
    }

    return reservationResult.deliveryReservation;
}

function applyDeliveryReservationToMergedResponse(merged, deliveryReservation) {
    if (!merged || !deliveryReservation) {
        return;
    }

    const assignedDeliveryDay = deliveryReservation.assignedDeliveryDay || null;
    if (!assignedDeliveryDay) {
        return;
    }

    if (merged?.ClientData?.data) {
        merged.ClientData.data.deliveryDay = assignedDeliveryDay;
    }
}

// Normalize CSV record keys/values to expected canonical keys
function normalizeClientRecord(raw) {
    const item = {};

    const normalizeKey = (k) => k.trim().toLowerCase().normalize('NFD').replace(/\p{Diacritic}/gu, '');

    const mapKey = (k) => {
        if (k.includes('direccion') && k.includes('despacho')) return 'Direccion Despacho';
        if (k.includes('direccion') && k.includes('facturacion')) return 'Direccion Facturacion';
        if (k.includes('comuna')) return 'Comuna Despacho';
        if (k.includes('region') || (k.includes('region') && k.includes('despacho')) || (k.includes('reg') && k.includes('despacho'))) return 'Region Despacho';
        if (k.includes('precio') && k.includes('caja 90')) return 'Precio Caja 90';
        if (k.includes('precio') && k.includes('90')) return 'Precio Caja 90';
        if (k.includes('precio') && k.includes('free')) return 'Precio Caja Free';
        if (k.includes('precio') && k.includes('caja')) return 'Precio Caja';
        if (k.includes('razon') || k.includes('razon social') || k.includes('razon_social') || k.includes('razon social')) return 'RAZ\u00d3N SOCIAL';
        if (k === 'name' || k.includes('company name')) return 'NAME';
        if (k.includes('rut')) return 'RUT';
        if (k.includes('email factura')) return 'EMAIL FACTURA';
        if (k.includes('email pedido')) return 'EMAIL PEDIDO';
        if (k.includes('diascredito') || k.includes('dias credito') || k.includes('dias credito')) return 'diasCredito';
        if (k.includes('orden') && k.includes('compra')) return 'Orden de compra (SI O NO)';
        if (k.includes('centro')) return 'CENTRO DE NEGOCIOS ';
        if (k.includes('vendedor')) return 'VENDEDOR ';
        return k; // fallback: keep original
    };

    Object.keys(raw).forEach((origKey) => {
        const nk = normalizeKey(origKey);
        const mapped = mapKey(nk);
        let val = raw[origKey];
        if (typeof val === 'string') {
            val = val.trim();
        }

        // Normalize price fields to numbers but keep original keys expected elsewhere
        if (mapped === 'Precio Caja' || mapped === 'Precio Caja 90' || mapped === 'Precio Caja Free') {
            if (typeof val === 'string' && val !== '') {
                const num = Number(val.replace(/[^0-9-]/g, ''));
                item[mapped] = isNaN(num) ? 0 : num;
            } else if (typeof val === 'number') {
                item[mapped] = val;
            } else {
                item[mapped] = 0;
            }
            return;
        }

        // Keep other fields as trimmed strings
        item[mapped] = val;
    });

    return item;
}

const CSV = './src/documents/KNOWLEDGEBASE.csv'; // Use the file path as a string

async function readCSV(req, res) {
    const results = [];
    const { rutToSearch, address } = req.query; // Get the RUT from the query parameters

    const normalizedRut = normalizeRut(rutToSearch); // Normalize the RUT
    console.log(`RUT to search: ${normalizedRut}`); // Log the RUT to search
    try {
        fs.createReadStream(CSV)
            .pipe(csvParser())
            .on('data', (data) => {
                if (data.RUT == normalizedRut) {
                    results.push(data);

                } // Collect all rows
            })
            .on('end', async () => {
                // console.log(results);

                console.log("***********************************************")

                // Normalize all records' keys and values
                for (let i = 0; i < results.length; i++) {
                    results[i] = normalizeClientRecord(results[i]);
                }

                if (results.length == 0) {
                    res.status(200).json({
                        data: [],
                        length: results.length,
                        address: address ? true : false,
                    })
                    return;
                }

                if (results.length == 1) {
                    // If only one result is found, return it directly
                    res.status(200).json({
                        data: results[0],
                        length: results.length,
                        address: true
                    });
                    return;
                }

                if (!address) {
                    // If no address is provided, return all results but address as false
                    const first = results[0];
                    first['Direccion Despacho'] = "";
                    res.status(200).json({
                        data: first,
                        length: results.length,
                        address: false
                    });
                    return;
                }

                //map results array for Gpt token limitation
                const clientData = results.map((item, index) => {
                    return {
                        index: index,
                        direccion: resolveClientDispatchAddress(item),
                    }
                });
                const gptResponse = await integrateWithChatGPT(clientData, address); // Integrate with ChatGPT

                if (gptResponse.length == 0) {

                    res.status(200).json({
                        data: gptResponse,
                        length: gptResponse.length,
                        address: address ? true : false,
                    })
                    return;
                }

                const matched = gptResponse
                    .filter((item) => item.match === true)
                    .sort((a, b) => Number(b?.confidence || 0) - Number(a?.confidence || 0))[0];
                const matchConfidence = Number(matched?.confidence || 0);
                const found = results.find((result, index) => {
                    return index == (matched?.index)
                });

                if (!found || matchConfidence < ADDRESS_MATCH_MIN_CONFIDENCE) {
                    console.log("no se encontro nada")
                    res.status(200).json({
                        data: found,
                        length: [found].length,
                        address: address ? true : false,
                        message: !found
                            ? "No se encontro nada"
                            : `Coincidencia descartada por baja confianza (${matchConfidence} < ${ADDRESS_MATCH_MIN_CONFIDENCE})`,
                        matchConfidence,
                        matchMinConfidence: ADDRESS_MATCH_MIN_CONFIDENCE
                    })
                    return;
                }
                // If a match is found, return the matched address

                res.status(200).json({
                    data: found,
                    length: [found].length,
                    address: true,
                    message: "Se encontro una coincidencia",
                    matchConfidence,
                    matchMinConfidence: ADDRESS_MATCH_MIN_CONFIDENCE
                });
                return;
            });
    } catch (error) {
        res.status(500).json({ error: 'Error reading the CSV file' });
    }
}

function getManualOcClientProfile(sourceClientCode) {
    const normalizedCode = String(sourceClientCode || '')
        .trim()
        .toUpperCase();
    return MANUAL_OC_CLIENT_PROFILES[normalizedCode] || null;
}

function parseExcelSerialDate(rawValue) {
    if (!/^\d{5,6}$/.test(String(rawValue || '').trim())) {
        return null;
    }

    const serial = Number(rawValue);
    if (!Number.isFinite(serial) || serial < 25000 || serial > 80000) {
        return null;
    }

    const parsed = XLSX?.SSF?.parse_date_code(serial);
    if (!parsed || !parsed.y || !parsed.m || !parsed.d) {
        return null;
    }

    const date = moment({
        year: parsed.y,
        month: parsed.m - 1,
        day: parsed.d
    });

    if (!date.isValid()) {
        return null;
    }

    const year = date.year();
    if (year < 2020 || year > 2100) {
        return null;
    }

    return date.format('YYYY-MM-DD');
}

function parseManualDateCandidate(value) {
    if (!value) {
        return null;
    }

    const rawValue = String(value).trim();
    if (!rawValue) {
        return null;
    }

    const excelSerialDate = parseExcelSerialDate(rawValue);
    if (excelSerialDate) {
        return excelSerialDate;
    }

    const normalized = rawValue
        .replace(/\./g, '/')
        .replace(/-/g, '/')
        .replace(/\s+/g, '');

    const formats = ['DD/MM/YYYY', 'D/M/YYYY', 'YYYY/MM/DD', 'DD/MM/YY', 'D/M/YY'];
    const parsed = moment(normalized, formats, true);
    if (!parsed.isValid()) {
        return null;
    }

    const year = parsed.year();
    if (year < 2020 || year > 2100) {
        return null;
    }

    return parsed.format('YYYY-MM-DD');
}

function detectOcDateFromText(text) {
    const sourceText = String(text || '');
    if (!sourceText.trim()) {
        return {
            date: null,
            confidence: 'none',
            method: 'empty_text'
        };
    }

    const labeledRegex = /fecha(?:\s+de)?(?:\s+oc|\s+orden|\s+pedido|\s+emision|\s+emisión|\s+po)?[\s:.\-|]{0,20}(\d{5,6}|\d{1,4}[\/\-.]\d{1,2}[\/\-.]\d{1,4})/i;
    const labeledMatch = sourceText.match(labeledRegex);
    if (labeledMatch && labeledMatch[1]) {
        const parsedDate = parseManualDateCandidate(labeledMatch[1]);
        if (parsedDate) {
            return {
                date: parsedDate,
                confidence: 'high',
                method: 'labeled_match',
                raw: labeledMatch[1]
            };
        }
    }

    const genericRegex = /(\b\d{5,6}\b|\d{4}[\/\-.]\d{1,2}[\/\-.]\d{1,2}|\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})/g;
    const candidates = sourceText.match(genericRegex) || [];
    for (const candidate of candidates) {
        const parsedDate = parseManualDateCandidate(candidate);
        if (parsedDate) {
            return {
                date: parsedDate,
                confidence: 'medium',
                method: 'generic_match',
                raw: candidate
            };
        }
    }

    return {
        date: null,
        confidence: 'none',
        method: 'not_found'
    };
}

function decodeFileBase64ToBuffer(fileBase64) {
    if (!fileBase64 || typeof fileBase64 !== 'string') {
        throw new Error('fileBase64 es requerido');
    }
    const payload = fileBase64.includes(',')
        ? fileBase64.split(',').pop()
        : fileBase64;

    return Buffer.from(payload || '', 'base64');
}

function columnIndexToExcelLabel(index) {
    let value = Number(index);
    let label = '';

    while (value > 0) {
        const remainder = (value - 1) % 26;
        label = String.fromCharCode(65 + remainder) + label;
        value = Math.floor((value - 1) / 26);
    }

    return label || 'A';
}

function formatPreviewNumber(value) {
    if (!Number.isFinite(value)) {
        return '';
    }

    if (Number.isInteger(value)) {
        return String(value);
    }

    if (Math.abs(value) < 1) {
        return value.toFixed(4).replace(/\.?0+$/, '');
    }

    return value.toFixed(2).replace(/\.?0+$/, '');
}

function normalizePreviewCell(rawCell) {
    if (rawCell === null || rawCell === undefined) {
        return '';
    }

    if (typeof rawCell === 'number') {
        return formatPreviewNumber(rawCell);
    }

    return String(rawCell).replace(/\s+/g, ' ').trim();
}

function formatExcelPreviewCell(rowValues, colIndex) {
    const rawValue = rowValues[colIndex];
    const normalized = normalizePreviewCell(rawValue);
    if (!normalized) {
        return '';
    }

    if (!/^\d{5,6}$/.test(normalized)) {
        return normalized;
    }

    const prevValue = colIndex > 0 ? normalizePreviewCell(rowValues[colIndex - 1]) : '';
    const nextValue = colIndex + 1 < rowValues.length ? normalizePreviewCell(rowValues[colIndex + 1]) : '';
    const rowHasFechaKeyword = rowValues.some((value) => /fecha/i.test(normalizePreviewCell(value)));
    const cellHasFechaNeighbor = /fecha/i.test(prevValue) || /fecha/i.test(nextValue);

    if (!rowHasFechaKeyword && !cellHasFechaNeighbor) {
        return normalized;
    }

    const parsedDate = parseExcelSerialDate(normalized);
    return parsedDate || normalized;
}

function normalizePreviewLabel(value) {
    return String(value || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/[^a-zA-Z0-9]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
        .toLowerCase();
}

function getCellValue(rowValues, index) {
    if (!Array.isArray(rowValues) || index < 0 || index >= rowValues.length) {
        return '';
    }
    return normalizePreviewCell(rowValues[index]);
}

function getNextNonEmptyCell(rowValues, fromIndex) {
    if (!Array.isArray(rowValues)) {
        return '';
    }

    for (let col = fromIndex + 1; col < rowValues.length; col += 1) {
        const value = normalizePreviewCell(rowValues[col]);
        if (value) {
            return value;
        }
    }

    return '';
}

function parseOptionalNumber(value) {
    if (value === null || value === undefined) {
        return null;
    }

    const text = String(value).trim();
    if (!text) {
        return null;
    }

    const normalized = text
        .replace(/\s+/g, '')
        .replace(/\$/g, '')
        .replace(/\u00A0/g, '')
        .replace(/\.(?=\d{3}\b)/g, '')
        .replace(',', '.');

    const cleaned = normalized.replace(/[^0-9.-]/g, '');
    if (!cleaned || cleaned === '-' || cleaned === '.' || cleaned === '-.') {
        return null;
    }

    const parsed = Number(cleaned);
    return Number.isFinite(parsed) ? parsed : null;
}

function parseOptionalDate(value) {
    const parsed = parseManualDateCandidate(value);
    return parsed || null;
}

function resolvePeyaMetadataKey(label) {
    const normalized = normalizePreviewLabel(label);
    if (!normalized) {
        return null;
    }

    if (normalized === 'store') return 'store';
    if (normalized === 'proveedor') return 'proveedor';
    if (normalized === 'razon social') return 'razonSocial';
    if (normalized === 'rut') return 'rut';
    if (normalized.startsWith('fecha emision')) return 'fechaEmision';
    if (normalized.startsWith('fecha entrega')) return 'fechaEntrega';
    if (normalized.startsWith('fecha venc po') || normalized.startsWith('fecha vencimiento po')) return 'fechaVencimientoPo';
    if (normalized.startsWith('direccion entrega')) return 'direccionEntrega';
    if (normalized === 'provincia') return 'provincia';
    if (normalized === 'pais') return 'pais';
    if (normalized.startsWith('horario recepcion')) return 'horarioRecepcion';
    if (normalized.startsWith('contacto encargado tienda')) return 'contactoEncargadoTienda';
    if (normalized.startsWith('costo total excl iva')) return 'costoTotalExclIva';
    if (normalized === 'iva') return 'ivaTotal';
    if (normalized.startsWith('costo total inc iva')) return 'costoTotalIncIva';

    return null;
}

function findPeyaItemHeaderRow(normalizedRows) {
    for (let rowIndex = 0; rowIndex < normalizedRows.length; rowIndex += 1) {
        const row = normalizedRows[rowIndex];
        const labels = row.map((cell) => normalizePreviewLabel(cell));
        // Spanish per-OC format
        const hasSku = labels.some((label) => label === 'n sku' || label === 'sku');
        const hasDescription = labels.some((label) => label === 'descripcion');
        const hasQty = labels.some((label) => label.includes('cantidad cajas') || label.includes('cantidad unidades'));
        if (hasSku && hasDescription && hasQty) {
            return rowIndex;
        }
        // English multi-OC SKU export format (sku_id / product_name / total_ordered_case)
        const hasSkuId = labels.some((label) => label === 'sku id');
        const hasProductName = labels.some((label) => label === 'product name');
        const hasOrderedCase = labels.some((label) => label === 'total ordered case' || label === 'ordered qty');
        if (hasSkuId && hasProductName && hasOrderedCase) {
            return rowIndex;
        }
    }

    return -1;
}

function mapPeyaItemColumns(headerRow) {
    const columnMap = {};
    headerRow.forEach((cell, colIndex) => {
        const label = normalizePreviewLabel(cell);
        if (!label) {
            return;
        }

        if (columnMap.sku === undefined && (label === 'n sku' || label === 'sku' || label === 'sku id')) {
            columnMap.sku = colIndex;
            return;
        }
        if (columnMap.ean === undefined && (label === 'ean' || label === 'barcode')) {
            columnMap.ean = colIndex;
            return;
        }
        if (columnMap.internalCode === undefined && (label.includes('codigo interno proveedor') || label === 'supplier sku')) {
            columnMap.internalCode = colIndex;
            return;
        }
        if (columnMap.description === undefined && (label === 'descripcion' || label === 'product name')) {
            columnMap.description = colIndex;
            return;
        }
        if (columnMap.quantityBoxes === undefined && (label.includes('cantidad cajas') || label === 'total ordered case')) {
            columnMap.quantityBoxes = colIndex;
            return;
        }
        if (columnMap.quantityUnits === undefined && (label.includes('cantidad unidades') || label === 'ordered qty')) {
            columnMap.quantityUnits = colIndex;
            return;
        }
        if (columnMap.unitCost === undefined && (label.includes('costo s unidad') || label.includes('costo unidad') || label === 'unit cost' || label === 'discounted unit cost')) {
            columnMap.unitCost = colIndex;
            return;
        }
        if (columnMap.costExclIva === undefined && (label.includes('costo excl iva') || label === 'net cost')) {
            columnMap.costExclIva = colIndex;
            return;
        }
        if (columnMap.iva === undefined && label === 'iva') {
            columnMap.iva = colIndex;
            return;
        }
        if (columnMap.costIncIva === undefined && label.includes('costo inc iva')) {
            columnMap.costIncIva = colIndex;
        }
    });

    return columnMap;
}

function extractPeyaExcelAnalysis(normalizedRows) {
    if (!Array.isArray(normalizedRows) || normalizedRows.length === 0) {
        return null;
    }

    const itemHeaderRowIndex = findPeyaItemHeaderRow(normalizedRows);
    const metadataRows = itemHeaderRowIndex >= 0
        ? normalizedRows.slice(0, itemHeaderRowIndex)
        : normalizedRows;
    const metadata = {};

    let purchaseOrderNumber = null;
    for (const row of metadataRows) {
        for (let colIndex = 0; colIndex < row.length; colIndex += 1) {
            const cell = normalizePreviewCell(row[colIndex]);
            if (!cell) {
                continue;
            }

            if (!purchaseOrderNumber && /^po\d{4,}$/i.test(cell)) {
                purchaseOrderNumber = cell.toUpperCase();
            }

            const metadataKey = resolvePeyaMetadataKey(cell);
            if (!metadataKey || metadata[metadataKey]) {
                continue;
            }

            metadata[metadataKey] = getNextNonEmptyCell(row, colIndex) || null;
        }
    }

    const items = [];
    if (itemHeaderRowIndex >= 0) {
        const headerRow = normalizedRows[itemHeaderRowIndex];
        const colMap = mapPeyaItemColumns(headerRow);
        let emptyStreak = 0;

        for (let rowIndex = itemHeaderRowIndex + 1; rowIndex < normalizedRows.length; rowIndex += 1) {
            const row = normalizedRows[rowIndex];
            const sku = getCellValue(row, colMap.sku);
            const ean = getCellValue(row, colMap.ean);
            const internalCode = getCellValue(row, colMap.internalCode);
            const description = getCellValue(row, colMap.description);
            const quantityBoxesRaw = getCellValue(row, colMap.quantityBoxes);
            const quantityUnitsRaw = getCellValue(row, colMap.quantityUnits);
            const unitCostRaw = getCellValue(row, colMap.unitCost);
            const costExclIvaRaw = getCellValue(row, colMap.costExclIva);
            const ivaRaw = getCellValue(row, colMap.iva);
            const costIncIvaRaw = getCellValue(row, colMap.costIncIva);

            const hasRelevantData = Boolean(
                sku
                || ean
                || internalCode
                || description
                || quantityBoxesRaw
                || quantityUnitsRaw
                || unitCostRaw
                || costExclIvaRaw
                || ivaRaw
                || costIncIvaRaw
            );

            if (!hasRelevantData) {
                emptyStreak += 1;
                if (emptyStreak >= 6) {
                    break;
                }
                continue;
            }

            emptyStreak = 0;
            if (!description && !sku && !ean) {
                continue;
            }

            items.push({
                rowNumber: rowIndex + 1,
                sku: sku || null,
                ean: ean || null,
                internalCode: internalCode || null,
                description: description || null,
                quantityBoxes: parseOptionalNumber(quantityBoxesRaw),
                quantityUnits: parseOptionalNumber(quantityUnitsRaw),
                unitCost: parseOptionalNumber(unitCostRaw),
                costExclIva: parseOptionalNumber(costExclIvaRaw),
                iva: parseOptionalNumber(ivaRaw),
                costIncIva: parseOptionalNumber(costIncIvaRaw)
            });
        }
    }

    const quantityBoxesTotal = items.reduce((acc, item) => acc + (item.quantityBoxes || 0), 0);
    const quantityUnitsTotal = items.reduce((acc, item) => acc + (item.quantityUnits || 0), 0);
    const costExclIvaTotal = items.reduce((acc, item) => acc + (item.costExclIva || 0), 0);
    const costIncIvaTotal = items.reduce((acc, item) => acc + (item.costIncIva || 0), 0);

    return {
        pattern: 'peya_excel_oc_v1',
        purchaseOrderNumber,
        metadata: {
            store: metadata.store || null,
            proveedor: metadata.proveedor || null,
            razonSocial: metadata.razonSocial || null,
            rut: metadata.rut || null,
            direccionEntrega: metadata.direccionEntrega || null,
            provincia: metadata.provincia || null,
            pais: metadata.pais || null,
            horarioRecepcion: metadata.horarioRecepcion || null,
            contactoEncargadoTienda: metadata.contactoEncargadoTienda || null
        },
        dates: {
            fechaEmision: parseOptionalDate(metadata.fechaEmision),
            fechaEntrega: parseOptionalDate(metadata.fechaEntrega),
            fechaVencimientoPo: parseOptionalDate(metadata.fechaVencimientoPo)
        },
        totals: {
            costoTotalExclIva: parseOptionalNumber(metadata.costoTotalExclIva),
            ivaTotal: parseOptionalNumber(metadata.ivaTotal),
            costoTotalIncIva: parseOptionalNumber(metadata.costoTotalIncIva)
        },
        items,
        itemStats: {
            count: items.length,
            quantityBoxesTotal: Math.round(quantityBoxesTotal * 1000) / 1000,
            quantityUnitsTotal: Math.round(quantityUnitsTotal * 1000) / 1000,
            costExclIvaTotal: Math.round(costExclIvaTotal * 1000) / 1000,
            costIncIvaTotal: Math.round(costIncIvaTotal * 1000) / 1000
        },
        rowMarkers: {
            itemHeaderRowNumber: itemHeaderRowIndex >= 0 ? itemHeaderRowIndex + 1 : null
        }
    };
}

function buildExcelPreviewFromBuffer(buffer) {
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    const firstSheetName = workbook.SheetNames[0] || '';
    const firstSheet = firstSheetName ? workbook.Sheets[firstSheetName] : null;
    if (!firstSheet) {
        return null;
    }

    const maxPreviewRows = Number.parseInt(process.env.MANUAL_OC_PREVIEW_ROWS || '24', 10);
    const maxPreviewCols = Number.parseInt(process.env.MANUAL_OC_PREVIEW_COLS || '0', 10);
    const maxReplicaRows = Number.parseInt(process.env.MANUAL_OC_REPLICA_MAX_ROWS || '140', 10);
    const maxReplicaCols = Number.parseInt(process.env.MANUAL_OC_REPLICA_MAX_COLS || '0', 10);
    const usePreviewColumnLimit = Number.isFinite(maxPreviewCols) && maxPreviewCols > 0;
    const useReplicaColumnLimit = Number.isFinite(maxReplicaCols) && maxReplicaCols > 0;
    const rows = XLSX.utils.sheet_to_json(firstSheet, {
        header: 1,
        defval: '',
        blankrows: false
    });

    const normalizedRows = rows
        .map((row) => row.map((cell) => normalizePreviewCell(cell)))
        .filter((row) => row.some((cell) => cell !== ''));
    const excelAnalysis = extractPeyaExcelAnalysis(normalizedRows);

    const trimmedRows = normalizedRows
        .slice(0, maxPreviewRows)
        .map((row, index) => ({
            rowNumber: index + 1,
            values: (usePreviewColumnLimit ? row.slice(0, maxPreviewCols) : row)
                .map((cell, colIndex) => formatExcelPreviewCell(row, colIndex))
        }))
        .filter((row) => row.values.some((cell) => normalizePreviewCell(cell) !== ''));

    const usedColumnIndexesSet = new Set();
    for (const row of trimmedRows) {
        row.values.forEach((cell, colIndex) => {
            if (normalizePreviewCell(cell) !== '') {
                usedColumnIndexesSet.add(colIndex);
            }
        });
    }

    const usedColumnIndexes = Array.from(usedColumnIndexesSet).sort((a, b) => a - b);
    const columns = usedColumnIndexes.map((index) => columnIndexToExcelLabel(index + 1));
    const rowNumbers = trimmedRows.map((row) => row.rowNumber);
    const previewRows = trimmedRows.map((row) => (
        usedColumnIndexes.map((colIndex) => row.values[colIndex] || '')
    ));

    const replicaSourceRows = normalizedRows
        .slice(0, maxReplicaRows)
        .map((row) => (useReplicaColumnLimit ? row.slice(0, maxReplicaCols) : row)
            .map((cell, colIndex) => formatExcelPreviewCell(row, colIndex)));

    const replicaUsedColumnsSet = new Set();
    for (const row of replicaSourceRows) {
        row.forEach((cell, colIndex) => {
            if (normalizePreviewCell(cell) !== '') {
                replicaUsedColumnsSet.add(colIndex);
            }
        });
    }
    const replicaUsedColumns = Array.from(replicaUsedColumnsSet).sort((a, b) => a - b);
    const replicaRows = replicaSourceRows.map((row) => replicaUsedColumns.map((colIndex) => row[colIndex] || ''));
    const replicaSheet = XLSX.utils.aoa_to_sheet(replicaRows);
    const rawHtml = XLSX.utils.sheet_to_html(replicaSheet, {
        id: 'manual-oc-excel-preview'
    });
    const safeHtml = String(rawHtml || '').replace(/<script[\s\S]*?<\/script>/gi, '');
    const rowsTruncated = normalizedRows.length > replicaRows.length;

    return {
        sheetName: firstSheetName,
        totalRows: normalizedRows.length,
        visibleRows: replicaRows.length,
        rowsTruncated,
        totalColumns: columns.length,
        columns,
        rowNumbers,
        rows: previewRows,
        html: safeHtml,
        analysis: excelAnalysis
    };
}

async function extractManualOcTextFromFile({ fileName, mimeType, fileBuffer }) {
    const safeFileName = String(fileName || '').trim().toLowerCase();
    const safeMimeType = String(mimeType || '').trim().toLowerCase();
    const isPdf = safeFileName.endsWith('.pdf') || safeMimeType.includes('pdf');
    const isExcel = /\.xlsx?$/i.test(safeFileName)
        || safeMimeType.includes('spreadsheetml')
        || safeMimeType.includes('ms-excel');

    if (isPdf) {
        return {
            fileType: 'pdf',
            text: await pdfBufferToText(fileBuffer),
            excelPreview: null
        };
    }

    if (isExcel) {
        return {
            fileType: 'excel',
            text: excelBufferToText(fileBuffer),
            excelPreview: buildExcelPreviewFromBuffer(fileBuffer)
        };
    }

    throw new Error('Tipo de archivo no soportado. Solo .xlsx, .xls o .pdf');
}

function buildManualReadEmailPayload({
    manualOcId,
    profile,
    fileName,
    excelText,
    emailDate,
    uploadedBy,
    targetOrderNumber = null
}) {
    const safeFileName = String(fileName || 'manual_oc.xlsx');
    const emailDateValue = String(emailDate || '').trim() || new Date().toISOString();
    return {
        emailBody: `[MANUAL_OC] Archivo ${safeFileName} cargado por ${uploadedBy || 'usuario_desconocido'}`,
        emailSubject: `[MANUAL_OC][${profile.sourceClientCode}] ${safeFileName}`,
        emailAttached: excelText,
        emailDate: emailDateValue,
        source: 'manual_portal',
        sender: profile.syntheticSender,
        attachmentFilename: safeFileName,
        targetOrderNumber: targetOrderNumber || null,
        manualOc: {
            id: manualOcId,
            sourceClientCode: profile.sourceClientCode,
            sourceClientName: profile.sourceClientName,
            parserProfile: profile.parserProfile,
            targetOrderNumber: targetOrderNumber || null
        }
    };
}

function getObjectValueByKeys(source, keys = []) {
    if (!source || typeof source !== 'object') {
        return null;
    }

    for (const key of keys) {
        if (!key) {
            continue;
        }
        if (Object.prototype.hasOwnProperty.call(source, key)) {
            const value = source[key];
            if (value !== null && value !== undefined && String(value).trim() !== '') {
                return value;
            }
        }
    }

    return null;
}

function parseNumberish(value) {
    if (value === null || value === undefined || value === '') {
        return 0;
    }

    if (typeof value === 'number') {
        return Number.isFinite(value) ? value : 0;
    }

    const asString = String(value).trim();
    if (!asString) {
        return 0;
    }

    const normalized = asString
        .replace(/\s+/g, '')
        .replace(/\$/g, '')
        .replace(/\u00A0/g, '')
        .replace(/\.(?=\d{3}\b)/g, '')
        .replace(',', '.');

    const cleaned = normalized.replace(/[^0-9.-]/g, '');
    if (!cleaned) {
        return 0;
    }

    const num = Number(cleaned);
    return Number.isFinite(num) ? num : 0;
}

function toPositiveInteger(value) {
    const parsed = parseNumberish(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return 0;
    }
    return Math.trunc(parsed);
}

function pickPreferredPrice(candidates = []) {
    let fallback = 0;
    for (const candidate of candidates) {
        const parsed = parseNumberish(candidate);
        if (parsed > 0) {
            return Math.round(parsed);
        }
        if (fallback === 0 && Number.isFinite(parsed)) {
            fallback = Math.round(parsed);
        }
    }
    return fallback;
}

function parseBooleanLoose(value, fallback = true) {
    if (typeof value === 'boolean') {
        return value;
    }

    if (value === null || value === undefined || value === '') {
        return fallback;
    }

    const normalized = String(value).trim().toLowerCase();
    if (['true', '1', 'si', 'sí', 'y', 'yes'].includes(normalized)) {
        return true;
    }
    if (['false', '0', 'no', 'n'].includes(normalized)) {
        return false;
    }
    return fallback;
}

function normalizeRegionCode(regionValue) {
    const normalized = String(regionValue || '')
        .toUpperCase()
        .normalize('NFD')
        .replace(/\p{Diacritic}/gu, '')
        .replace(/\s+/g, ' ')
        .trim();

    if (!normalized) {
        return '';
    }
    if (normalized === 'RM' || normalized.includes('METROPOLITANA') || normalized === 'SANTIAGO') {
        return 'RM';
    }
    if (normalized === 'V' || normalized === 'VALPARAISO' || normalized.includes('VALPARAISO')) {
        return 'V';
    }
    if (normalized === 'VI' || normalized.includes("O'HIGGINS") || normalized.includes('OHIGGINS')) {
        return 'VI';
    }

    return normalized;
}

function parseDateToDateObject(value) {
    if (!value && value !== 0) {
        return null;
    }

    if (value instanceof Date && !Number.isNaN(value.getTime())) {
        return value;
    }

    const directDate = new Date(value);
    if (!Number.isNaN(directDate.getTime())) {
        return directDate;
    }

    const text = String(value).trim();
    if (!text) {
        return null;
    }

    const parsed = moment(text, [
        'DD-MM-YYYY HH:mm:ss',
        'D-M-YYYY HH:mm:ss',
        'DD/MM/YYYY HH:mm:ss',
        'D/M/YYYY HH:mm:ss',
        'YYYY-MM-DD HH:mm:ss',
        'YYYY-MM-DDTHH:mm:ss.SSSZ',
        'YYYY-MM-DDTHH:mm:ssZ',
        'YYYY-MM-DDTHH:mm:ss',
        'YYYY-MM-DD'
    ], true);

    if (!parsed.isValid()) {
        return null;
    }

    return parsed.toDate();
}

function toChileTimestampParts(value, options = {}) {
    const fallbackToNow = options?.fallbackToNow === true;
    let parsedDate = null;

    if (value !== null && value !== undefined && value !== '') {
        parsedDate = parseDateToDateObject(value);
    } else if (fallbackToNow) {
        parsedDate = new Date();
    }

    if (!parsedDate) {
        return null;
    }

    const formatter = new Intl.DateTimeFormat('sv-SE', {
        timeZone: 'America/Santiago',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
    });

    const map = {};
    formatter.formatToParts(parsedDate).forEach((part) => {
        if (part.type !== 'literal') {
            map[part.type] = part.value;
        }
    });

    const date = `${map.year}-${map.month}-${map.day}`;
    const time = `${map.hour}:${map.minute}:${map.second}`;
    return {
        date,
        time,
        dateTime: `${date} ${time}`
    };
}

function normalizeManualOcArrivalMeridiem(value) {
    const normalized = String(value || '').trim().toLowerCase();
    if (normalized === 'pm') {
        return 'pm';
    }
    return 'am';
}

function buildManualOcArrivalDateTime({
    arrivalDate,
    arrivalMeridiem,
    fallbackDate,
    cutoffHourOverride = null
}) {
    const dateValue = parseManualDateCandidate(arrivalDate || fallbackDate);
    if (!dateValue) {
        return null;
    }

    const meridiem = normalizeManualOcArrivalMeridiem(arrivalMeridiem);
    const cutoffHourSource = Number.isFinite(cutoffHourOverride)
        ? cutoffHourOverride
        : MANUAL_OC_DISPATCH_CUTOFF_HOUR;
    const cutoffHour = Number.isFinite(cutoffHourSource)
        ? Math.min(Math.max(Math.trunc(cutoffHourSource), 0), 23)
        : 12;
    const cutoffMoment = moment.tz(
        `${dateValue} ${String(cutoffHour).padStart(2, '0')}:00:00`,
        'YYYY-MM-DD HH:mm:ss',
        'America/Santiago'
    );
    const selectedMoment = meridiem === 'pm'
        ? cutoffMoment.clone().add(1, 'minute')
        : cutoffMoment.clone().subtract(1, 'minute');
    const dateTimeIso = selectedMoment.format('YYYY-MM-DDTHH:mm:ssZ');
    return {
        date: dateValue,
        meridiem,
        dateTimeIso
    };
}

function resolveManualOcDispatchCutoffHour({ clientData, emailData }) {
    const comuna = getObjectValueByKeys(emailData, [
        'Comuna',
        'Comuna_despacho',
        'Comuna despacho'
    ]) || getObjectValueByKeys(clientData, [
        'Comuna Despacho',
        'Comuna despacho',
        'Comuna'
    ]) || '';

    const region = getObjectValueByKeys(emailData, [
        'region',
        'Region',
        'Region',
        'Region',
        'Region'
    ]) || getObjectValueByKeys(clientData, [
        'region',
        'Region Despacho',
        'Region Despacho',
        'Region Despacho',
        'Region Despacho'
    ]) || '';

    return resolveDispatchCutoffHourByComuna(comuna, region);
}

function resolveManualOcDeliveryDay({
    deliveryReservation,
    clientData,
    emailData,
    ocDateConfirmed,
    arrivalDateTime
}) {
    const emissionDate = parseManualDateCandidate(
        ocDateConfirmed
        || getObjectValueByKeys(emailData, ['OC_date', 'OC Date', 'Fecha_emision', 'Fecha emision'])
        || getObjectValueByKeys(clientData, ['OC_date', 'OC Date', 'Fecha_emision', 'Fecha emision'])
    );

    const comunaCandidates = [
        getObjectValueByKeys(emailData, [
            'Comuna',
            'Comuna_despacho',
            'Comuna despacho'
        ]),
        getObjectValueByKeys(clientData, [
            'Comuna Despacho',
            'Comuna despacho',
            'Comuna'
        ])
    ]
        .map((value) => String(value || '').trim())
        .filter((value, index, array) => value !== '' && array.indexOf(value) === index);

    const normalizedArrivalDateTime = String(arrivalDateTime || '').trim();
    const regionForCalculation = getObjectValueByKeys(emailData, [
        'region',
        'Region',
        'Region',
        'Region',
        'Region'
    ]) || getObjectValueByKeys(clientData, [
        'region',
        'Region Despacho',
        'Region Despacho',
        'Region Despacho',
        'Region Despacho'
    ]);
    const arrivalDateTimeIsValid = normalizedArrivalDateTime
        && moment(normalizedArrivalDateTime, moment.ISO_8601, true).isValid();
    const dateTimeForCalculation = arrivalDateTimeIsValid
        ? normalizedArrivalDateTime
        : (emissionDate ? `${emissionDate}T09:00:00-03:00` : null);

    if (dateTimeForCalculation && comunaCandidates.length > 0) {
        for (const comuna of comunaCandidates) {
            const calculatedDeliveryDay = findDeliveryDayByComuna(
                comuna,
                dateTimeForCalculation,
                regionForCalculation
            );
            if (calculatedDeliveryDay) {
                return String(calculatedDeliveryDay).trim();
            }
        }
    }

    const reservedDeliveryDay = String(deliveryReservation?.assignedDeliveryDay || '').trim();
    if (reservedDeliveryDay) {
        return reservedDeliveryDay;
    }

    const csvDeliveryDay = String(getObjectValueByKeys(clientData, ['deliveryDay']) || '').trim();
    if (csvDeliveryDay) {
        return csvDeliveryDay;
    }

    return null;
}

function buildManualOcBillingPayload({ mergedResult, ocDateConfirmed, arrivalDateTime }) {
    const merged = mergedResult?.merged || {};
    const emailData = merged?.EmailData || {};
    const clientData = merged?.ClientData?.data || merged?.ClientData || {};
    const deliveryReservation = mergedResult?.deliveryReservation || null;

    const price150 = pickPreferredPrice([
        getObjectValueByKeys(clientData, ['Precio Caja']),
        emailData?.precio_caja
    ]);
    const price90 = pickPreferredPrice([
        getObjectValueByKeys(clientData, ['Precio Caja 90']),
        emailData?.precio_caja_90g
    ]);
    const priceFree = pickPreferredPrice([
        getObjectValueByKeys(clientData, ['Precio Caja Free']),
        emailData?.precio_caja_free
    ]);

    const details = MANUAL_OC_DETAIL_MAPPING.map((item) => {
        let unitPrice = 0;
        if (item.priceBucket === '150') {
            unitPrice = price150;
        } else if (item.priceBucket === '90') {
            unitPrice = price90;
        } else if (item.priceBucket === 'free') {
            unitPrice = priceFree;
        }

        return {
            code: item.code,
            price: unitPrice,
            quantity: toPositiveInteger(emailData?.[item.quantityKey])
        };
    });

    const regionRaw = getObjectValueByKeys(clientData, [
        'region',
        'Region Despacho',
        'Region Despacho'
    ]);
    const gloss = getObjectValueByKeys(emailData, [
        'Direccion_despacho',
        'Direccion_despacho'
    ]) || getObjectValueByKeys(clientData, [
        'Direccion Despacho',
        'Direccion Despacho'
    ]) || '';

    const paymentConditionRaw = getObjectValueByKeys(clientData, [
        'diasCredito',
        'Días Crédito',
        'Dias Credito'
    ]);
    const sellerFileId = getObjectValueByKeys(clientData, ['VENDEDOR ', 'VENDEDOR']) || '';
    const businessCenter = getObjectValueByKeys(clientData, ['CENTRO DE NEGOCIOS ', 'CENTRO DE NEGOCIOS']) || '';
    const deliveryDay = resolveManualOcDeliveryDay({
        deliveryReservation,
        clientData,
        emailData,
        ocDateConfirmed,
        arrivalDateTime: arrivalDateTime || merged?.emailDate || null
    });
    const clientFile = getObjectValueByKeys(emailData, ['Rut', 'RUT'])
        || getObjectValueByKeys(clientData, ['RUT', 'Rut'])
        || '';

    return {
        giro: MANUAL_OC_DEFAULT_GIRO,
        gloss: String(gloss || '').trim(),
        region: normalizeRegionCode(regionRaw),
        shopId: MANUAL_OC_DEFAULT_SHOP_ID,
        details,
        storage: MANUAL_OC_DEFAULT_STORAGE,
        lastFolio: 0,
        priceList: MANUAL_OC_DEFAULT_PRICE_LIST,
        clientFile: String(clientFile || '').trim(),
        firstFolio: 0,
        isDelivery: parseBooleanLoose(emailData?.isDelivery, true),
        deliveryDay: deliveryDay ? String(deliveryDay).trim() : null,
        customFields: [],
        documentType: MANUAL_OC_DEFAULT_DOCUMENT_TYPE,
        sellerFileId: String(sellerFileId || '').trim(),
        reservationId: deliveryReservation?.reservationId || null,
        businessCenter: String(businessCenter || '').trim(),
        paymentCondition: paymentConditionRaw !== null && paymentConditionRaw !== undefined
            ? String(paymentConditionRaw).trim()
            : '',
        attachedDocuments: [],
        ventaRecDesGlobal: [],
        isTransferDocument: true
    };
}

/**
 * Replaces garbled multi-encoded UTF-8 keys with clean ASCII equivalents.
 * The CSV headers were stored with incorrect encoding (UTF-8 bytes interpreted
 * as Latin-1 multiple times), producing keys like 'DirecciÃƒÆ’Ã†'Ãƒâ€šÃ‚Â³n Despacho'.
 * Strategy: strip all non-ASCII characters from the key, then match by the
 * remaining ASCII skeleton (e.g. 'Direccin Despacho' â†’ 'Direccion Despacho').
 */
function sanitizeClientDataKeys(obj) {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
        return obj;
    }
    // Strip non-ASCII chars, lowercase, collapse spaces
    const toAsciiSkeleton = (s) => s
        .replace(/[^\x00-\x7F]/g, '')
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .trim();

    const result = {};
    for (const [key, value] of Object.entries(obj)) {
        const sk = toAsciiSkeleton(key);
        let cleanKey = key;
        if (sk.includes('direcci') && sk.includes('despacho')) {
            cleanKey = 'Direccion Despacho';
        } else if (sk.includes('direcci') && (sk.includes('factura') || sk.includes('facturaci'))) {
            cleanKey = 'Direccion Facturacion';
        } else if (sk.includes('regi') && sk.includes('despacho')) {
            cleanKey = 'Region Despacho';
        } else if (sk.includes('raz') && sk.includes('social')) {
            cleanKey = 'RAZON SOCIAL';
        }
        result[cleanKey] = value;
    }
    return result;
}

async function sendManualMergedToMake({
    manualOcId,
    profile,
    mergedResult,
    ocDateDetected,
    ocDateConfirmed,
    arrivalDate,
    arrivalMeridiem,
    arrivalDateTime,
    uploadedBy,
    fileMeta,
    developerMode = false,
    submitRequest = null,
    makeOptions = null
}) {
    const merged = mergedResult?.merged || null;
    const deliveryReservation = mergedResult?.deliveryReservation || null;
    const emailData = merged?.EmailData || null;
    const clientData = sanitizeClientDataKeys(merged?.ClientData?.data || merged?.ClientData || null);
    const billingPayload = buildManualOcBillingPayload({
        mergedResult,
        ocDateConfirmed,
        arrivalDateTime
    });

    const builtAtChile = toChileTimestampParts(null, { fallbackToNow: true });
    const receivedAtChile = toChileTimestampParts(
        merged?.emailDate
        || (ocDateConfirmed ? `${ocDateConfirmed}T12:00:00-03:00` : null)
    );
    const parserExecutionAtChile = toChileTimestampParts(merged?.executionDate || null);

    const resolvedMakeMode = String(makeOptions?.mode || MANUAL_OC_MAKE_MODE_DEFAULT).trim() || MANUAL_OC_MAKE_MODE_DEFAULT;
    const resolvedTestMode = parseBooleanLoose(makeOptions?.testMode, MANUAL_OC_MAKE_TEST_MODE_DEFAULT);
    const resolvedPreventBilling = parseBooleanLoose(
        makeOptions?.preventBilling,
        MANUAL_OC_MAKE_PREVENT_BILLING_DEFAULT
    );

    const makePayload = {
        source: 'manual_oc',
        mode: resolvedMakeMode,
        testMode: resolvedTestMode,
        preventBilling: resolvedPreventBilling,
        manualOcId,
        sourceClientCode: profile.sourceClientCode,
        sourceClientName: profile.sourceClientName,
        parserProfile: profile.parserProfile,
        uploadedBy: uploadedBy || 'usuario_desconocido',
        fileMeta,
        ocDateDetected: ocDateDetected || null,
        ocDateConfirmed: ocDateConfirmed || null,
        arrivalDate: arrivalDate || null,
        arrivalMeridiem: arrivalMeridiem || null,
        arrivalDateTime: arrivalDateTime || null,
        sentAt: new Date().toISOString(),
        timingChile: {
            payloadBuiltAt: builtAtChile?.dateTime || null,
            payloadBuiltDate: builtAtChile?.date || null,
            payloadBuiltTime: builtAtChile?.time || null,
            parserExecutionAt: parserExecutionAtChile?.dateTime || null,
            parserExecutionDate: parserExecutionAtChile?.date || null,
            parserExecutionTime: parserExecutionAtChile?.time || null,
            sourceReceivedAt: receivedAtChile?.dateTime || null,
            sourceReceivedDate: receivedAtChile?.date || null,
            sourceReceivedTime: receivedAtChile?.time || null
        },
        composition: {
            emailData,
            clientData,
            submitRequest: submitRequest || null,
            mergedMeta: {
                executionDate: merged?.executionDate || null,
                ocDate: merged?.OC_date || null,
                emailDate: merged?.emailDate || null,
                hasMatch: merged?.hasMatch ?? null
            },
            deliveryReservation,
            uploadedBy: uploadedBy || 'usuario_desconocido'
        },
        billingPayload,
        merged: merged ? {
            ...merged,
            ClientData: merged.ClientData
                ? {
                    ...merged.ClientData,
                    data: sanitizeClientDataKeys(merged.ClientData?.data || merged.ClientData)
                }
                : merged.ClientData
        } : null,
        deliveryReservation
    };

    if (developerMode === true) {
        await fs.promises.mkdir(MANUAL_OC_DEVELOPER_OUTBOX_DIR, { recursive: true });
        const safeManualOcId = String(manualOcId || 'manual_oc').replace(/[^a-zA-Z0-9_-]/g, '_');
        const dumpFileName = `${new Date().toISOString().replace(/[:.]/g, '-')}_${safeManualOcId}.json`;
        const dumpPath = path.join(MANUAL_OC_DEVELOPER_OUTBOX_DIR, dumpFileName);
        await fs.promises.writeFile(dumpPath, JSON.stringify(makePayload, null, 2), 'utf8');
        return {
            delivered: false,
            skipped: true,
            reason: 'developer_mode_payload_dumped',
            developerMode: true,
            payloadDumpPath: dumpPath
        };
    }

    if (!MAKE_MANUAL_OC_WEBHOOK_URL) {
        return {
            delivered: false,
            skipped: true,
            reason: 'missing_make_webhook_url'
        };
    }

    const makeResponse = await fetch(MAKE_MANUAL_OC_WEBHOOK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(makePayload)
    });

    const responseText = await makeResponse.text();
    return {
        delivered: makeResponse.ok,
        skipped: false,
        status: makeResponse.status,
        statusText: makeResponse.statusText,
        responseText
    };
}

/**
 * Fixes unescaped double-quotes that sit inside JSON string values.
 * Strategy: walk the string char-by-char; when inside a string and we hit a `"`,
 * look-ahead (skipping whitespace) to see if the next meaningful character is one
 * of `,`, `}`, `]`, `:`. If so it is a structural (closing) quote; otherwise
 * it is an internal quote that must be escaped with `\`.
 */
function fixUnescapedJsonQuotes(str) {
    try {
        JSON.parse(str);
        return str; // already valid
    } catch (_) {
        // needs fixing
    }

    const result = [];
    let inString = false;

    for (let i = 0; i < str.length; i++) {
        const ch = str[i];

        // Pass through already-escaped characters inside a string
        if (ch === '\\' && inString) {
            result.push(ch);
            if (i + 1 < str.length) {
                result.push(str[i + 1]);
                i++;
            }
            continue;
        }

        if (ch === '"') {
            if (!inString) {
                // Opening quote
                inString = true;
                result.push(ch);
            } else {
                // Look-ahead: skip whitespace, check next meaningful char
                let j = i + 1;
                while (j < str.length && /\s/.test(str[j])) j++;
                const next = str[j] || '';

                if (next === ',' || next === '}' || next === ']' || next === ':' || j >= str.length) {
                    // Structural closing quote
                    inString = false;
                    result.push(ch);
                } else {
                    // Internal quote - escape it
                    result.push('\\');
                    result.push(ch);
                }
            }
        } else {
            result.push(ch);
        }
    }

    return result.join('');
}

async function readEmailBody(req, res) {

    const plainText = req.body;
    try {
        // console.log("hola",req.body)
        // console.log("Received plainText:", plainText);

        // Sanitize the email body:
        // 1. Collapse all whitespace (newlines, tabs, etc.) to single spaces
        // 2. Fix any unescaped double-quotes inside JSON string values
        const sanitizedEmailBody = fixUnescapedJsonQuotes(
            plainText
                .replaceAll(/\s+/g, ' ') // Remove all white spaces
                .trim() // Trim leading and trailing spaces
        );
        
            console.log("sanitizedEmailBody", sanitizedEmailBody);

        const parsedRequestBody = JSON.parse(sanitizedEmailBody);

        const {
            emailBody,
            emailSubject,
            emailAttached,
            emailDate,
            source,
            keyLogistics,
            rappiTurbo,
            sender,
            attachmentFilename
        } = parsedRequestBody; // Parse the sanitized email body

        console.log(parsedRequestBody);

        // res.json({analyzeOrderEmail});
        // return

        // if (emailBody == null || emailSubject == null || emailAttached == null) {
        //     console.log("Invalid request emailBody:", emailBody);
        //     console.log("Invalid request emailSubject:", emailSubject);
        //     console.log("Invalid request emailAttached:", emailAttached);
        //     // Return an error response if any of the required fields are missing

        //     return res.status(400).json({ error: 'Invalid request body' });
        // }

        const requiredFields = ['emailBody', 'emailSubject', 'emailAttached', 'emailDate'];

        const missingFields = requiredFields.filter((field) => !(field in parsedRequestBody));

        if (missingFields.length > 0) {
            console.log("Invalid request, missing fields:", missingFields);
            return res.status(400).json({ error: 'Invalid request body' });
        }

        let attachedPrompt = ""
        let OC = ""
        if (emailAttached !== "") {
            attachedPrompt = `y el texto que hemos extraido desde un PDF adjunto que trae la orden de compra con el pedido: "${emailAttached}". `
        }

        const systemPrompt = `Devuélveme exclusivamente un JSON válido, sin explicaciones ni texto adicional.
        La respuesta debe comenzar directamente con [ y terminar con ].
        No incluyas ningún texto antes o después del JSON.
        No uses formato Markdown. 
        No expliques lo que estás haciendo.
        Tu respuesta debe ser solamente el JSON. Nada más.;`;

        // const userPrompt = `Eres un bot que analiza pedidos para Franuí, empresa que comercializa frambuesas bañadas en chocolate. Franuí maneja solamente 3 productos
        //     Frambuesas bañadas en chocolate amargo
        //     Frambuesas bañadas en chocolate de leche
        //     Frambuesas bañadas en chocolate pink

        //     Debes analizar el texto del body del correo ${emailBody}, el asunto ${emailSubject} y cualquier información contenida en ${attachedPrompt} para extraer los datos relevantes y guardarlos en variables

        //     Nuestro negocio se llama Olimpia SPA y nuestro rut es 77.419.327-8. Ninguna variable extraída debe contener la palabra Olimpia ni nuestro RUT

        //     Importante el campo Rut es obligatorio y prioritario. Si no se encuentra, la ejecución es inválida
        //     Debes buscar el primer RUT que no sea el de Olimpia SPA 77.419.327-8
        //     Los formatos posibles son
        //     xx.xxx.xxx-x
        //     xxx.xxx.xxx-x
        //     xxxxxxxx-x
        //     El RUT puede encontrarse en cualquier parte del correo o asunto
        //     No devuelvas el RUT si es igual a 77.419.327-8 y continúa buscando hasta encontrar uno válido
        //     Si no encuentras ningún otro RUT válido, devuelve null

        //     Debes extraer los siguientes datos
        //     Razon_social contiene la razón social del cliente
        //     Direccion_despacho dirección a la cual se enviarán los productos. Si no la encuentras, devuelve null
        //     Comuna comuna de despacho. Si no la encuentras, devuelve null
        //     Rut ver reglas anteriores
        //     Pedido_Cantidad_Pink cantidad de cajas de chocolate pink. Si no existe, devuelve 0
        //     Pedido_Cantidad_Amargo: cantidad de cajas de chocolate amargo. Si no existe, devuelve 0
        //     Pedido_Cantidad_Leche: cantidad de cajas de chocolate de leche. Si no existe, devuelve 0
        //     Pedido_PrecioTotal_Pink: devuelve 0
        //     Pedido_PrecioTotal_Amargo monto total del pedido de chocolate amargo. Si no existe, devuelve 0
        //     Pedido_PrecioTotal_Leche monto total del pedido de chocolate de leche. Si no existe, devuelve 0
        //     Orden_de_Compra número de orden de compra. Si no existe, devuelve null
        //     Monto neto también llamado subtotal. Si no existe, devuelve 0
        //     Iva monto del impuesto. Si no existe, devuelve 0
        //     Total monto total del pedido incluyendo impuestos. Si no existe, devuelve 0
        //     Sender_Email correo electrónico del remitente del mensaje
        //     precio_caja precio de la caja de chocolate pink amargo o leche. Si no existe, devuelve 0
        //     URL_ADDRESS dirección de despacho codificada en formato URL lista para usarse en una petición HTTP GET. No devuelvas nada más que la cadena codificada sin explicaciones ni comillas
        //     PaymentMethod
        //     method en caso de hacer referencia a un cheque devolver letra C en caso contrario devuelve vacío
        //     paymentsDays número de días de pago si se menciona. En caso contrario devuelve vacío
        //     isDelivery en caso de que el pedido sea para delivery devuelve true si no es para delivery devuelve false

        //     Reglas para campo Razon_social
        //     Puede estar en el cuerpo del correo o en el asunto
        //     En caso de no haber una indicación clara puede estar mencionada como sucursal local o cliente

        //     Reglas para Direccion_despacho
        //     Puede estar en el cuerpo del correo o en el asunto
        //     Debe incluir calle y comuna
        //     Si no se menciona dirección específica puede estar indicada como sucursal o local
        //     Si el pedido es para retiro reemplaza este valor por la palabra RETIRO

        //     Reglas para precio_caja
        //     El precio de la caja ronda entre los 60000 y 80000 pesos
        //     Debe ser el mismo para pink amargo y leche
        //     Si no se encuentra en el texto devuelve 0

        //     Reglas para isDelivery
        //     Si el pedido es para retiro en sucursal devolver false
        //     Si no se menciona retiro explícitamente devolver true
        //     Ejemplos de retiro
        //     te quiero hacer un pedido para retirar este viernes
        //     pedido con retiro
        //     En caso de duda devolver true por defecto
        // `

        const userPrompt =
            `Eres un bot que analiza pedidos para Franuí, empresa que comercializa frambuesas bañadas en chocolate.

Franuí maneja los siguientes productos:

=== PRODUCTOS DE 150 GRAMOS (24 unidades por caja) ===
- Frambuesas bañadas en chocolate amargo
- Frambuesas bañadas en chocolate de leche
- Frambuesas bañadas en chocolate pink
- Franuí Chocolate Free (sin azúcar)

=== PRODUCTOS DE 90 GRAMOS (18 unidades por caja) ===
- Caja Franui Amargo 90 gramos
- Caja Franui Leche 90 gramos
- Caja Franui Pink 90 gramos

IMPORTANTE: Si el producto NO especifica "90g" o "90 gramos", se asume que es el producto de 150 gramos.

Debes analizar el texto del body del correo ${emailBody}, el asunto ${emailSubject} y cualquier información contenida en ${attachedPrompt} para extraer los datos relevantes y guardarlos en variables

Nuestro negocio se llama Olimpia SPA y nuestro rut es 77.419.327-8. Ninguna variable extraída debe contener la palabra Olimpia ni nuestro RUT

Importante el campo Rut es obligatorio y prioritario. Si no se encuentra, la ejecución es inválida
Debes buscar el primer RUT que no sea el de Olimpia SPA 77.419.327-8
Los formatos posibles son
xx.xxx.xxx-x
xxx.xxx.xxx-x
xxxxxxxx-x
El RUT puede encontrarse en cualquier parte del correo o asunto
No devuelvas el RUT si es igual a 77.419.327-8 y continúa buscando hasta encontrar uno válido
Si no encuentras ningún otro RUT válido, devuelve null

Debes extraer los siguientes datos:

=== DATOS DEL CLIENTE ===
Razon_social: contiene la razón social del cliente
Direccion_despacho: dirección PRINCIPAL de despacho. Priorizar la que diga "despacho", "entrega" o "envío". Si no la encuentras, devuelve null
Direcciones_encontradas: ARRAY con TODAS las direcciones encontradas en el documento (facturación, despacho, entrega, etc). Esto es MUY IMPORTANTE para poder buscar coincidencias. Ejemplo: ["NUEVA LOS LEONES 030 LOCAL 16", "AVDA COSTANERA SUR 2710 PISO 12"]
Comuna: comuna de despacho. Si no la encuentras, devuelve null
Rut: ver reglas anteriores

=== CANTIDADES DE PRODUCTOS 150g (24 unidades por caja) ===
Pedido_Cantidad_Pink: cantidad de cajas de chocolate pink 150g. Si no existe, devuelve 0
Pedido_Cantidad_Amargo: cantidad de cajas de chocolate amargo 150g. Si no existe, devuelve 0
Pedido_Cantidad_Leche: cantidad de cajas de chocolate de leche 150g. Si no existe, devuelve 0
Pedido_Cantidad_Free: cantidad de cajas de Franuí Chocolate Free (sin azúcar) 150g. Si no existe, devuelve 0

=== CANTIDADES DE PRODUCTOS 90g (18 unidades por caja) ===
Pedido_Cantidad_Pink_90g: cantidad de cajas de chocolate pink 90g. Si no existe, devuelve 0
Pedido_Cantidad_Amargo_90g: cantidad de cajas de chocolate amargo 90g. Si no existe, devuelve 0
Pedido_Cantidad_Leche_90g: cantidad de cajas de chocolate de leche 90g. Si no existe, devuelve 0

=== PRECIOS PRODUCTOS 150g ===
Pedido_PrecioTotal_Pink: monto total del pedido de chocolate pink 150g. Si no existe, devuelve 0
Pedido_PrecioTotal_Amargo: monto total del pedido de chocolate amargo 150g. Si no existe, devuelve 0
Pedido_PrecioTotal_Leche: monto total del pedido de chocolate de leche 150g. Si no existe, devuelve 0
Pedido_PrecioTotal_Free: monto total del pedido de Franuí Chocolate Free 150g. Si no existe, devuelve 0

=== PRECIOS PRODUCTOS 90g ===
Pedido_PrecioTotal_Pink_90g: monto total del pedido de chocolate pink 90g. Si no existe, devuelve 0
Pedido_PrecioTotal_Amargo_90g: monto total del pedido de chocolate amargo 90g. Si no existe, devuelve 0
Pedido_PrecioTotal_Leche_90g: monto total del pedido de chocolate de leche 90g. Si no existe, devuelve 0

=== DATOS DE LA ORDEN ===
Orden_de_Compra: número de orden de compra. Si no existe, devuelve null
Monto: neto también llamado subtotal. Si no existe, devuelve 0
Iva: monto del impuesto. Si no existe, devuelve 0
Total: monto total del pedido incluyendo impuestos. Si no existe, devuelve 0
Sender_Email: correo electrónico del remitente del mensaje

=== PRECIOS POR CAJA ===
precio_caja: precio de la caja de chocolate pink, amargo o leche 150g. Si no existe, devuelve 0
precio_caja_90g: precio de la caja de productos 90g. Si no existe, devuelve 0
precio_caja_free: precio de la caja de Franuí Chocolate Free. Si no existe, devuelve 0

URL_ADDRESS: dirección de despacho codificada en formato URL lista para usarse en una petición HTTP GET. No devuelvas nada más que la cadena codificada sin explicaciones ni comillas

PaymentMethod:
method: en caso de hacer referencia a un cheque devolver letra C, en caso contrario devuelve vacío
paymentsDays: número de días de pago si se menciona. En caso contrario devuelve vacío

isDelivery: en caso de que el pedido sea para delivery devuelve true, si no es para delivery devuelve false

=== REGLAS ESPECÍFICAS ===

Reglas para campo Razon_social:
Puede estar en el cuerpo del correo o en el asunto
En caso de no haber una indicación clara puede estar mencionada como sucursal local o cliente

Reglas para Direccion_despacho:
Puede estar en el cuerpo del correo o en el asunto
Debe incluir calle y comuna
Si no se menciona dirección específica puede estar indicada como sucursal o local
Si el pedido es para retiro reemplaza este valor por la palabra RETIRO
PRIORIDAD: Si hay múltiples direcciones, priorizar la que esté etiquetada como "despacho", "entrega" o "envío" sobre la de "facturación"

Reglas para Direcciones_encontradas:
Debe ser un ARRAY con TODAS las direcciones físicas encontradas en el documento
Incluir tanto direcciones de facturación como de despacho
No incluir direcciones de correo electrónico
No incluir direcciones web/URL
Ejemplo: si el documento dice "Direccion: NUEVA LOS LEONES 030" y "Direccion: AVDA COSTANERA SUR 2710", devolver ["NUEVA LOS LEONES 030", "AVDA COSTANERA SUR 2710"]
Si solo hay una dirección, devolver array con un elemento
Si no hay direcciones, devolver array vacío []

Reglas para identificar productos de 90g:
Buscar menciones de "90g", "90 gramos", "90gr" en el nombre del producto
Ejemplos: "Franui Leche 90g", "Caja Franui Pink 90 gramos", "Amargo 90g"
Si NO especifica gramos, asumir que es producto de 150g

Reglas para identificar Franuí Chocolate Free:
Buscar menciones de "Free", "Chocolate Free", "sin azúcar"
Ejemplos: "Franuí Chocolate Free", "Franui Free", "Caja Franui Free"

Reglas para precio_caja (150g):
El precio de la caja ronda entre los 60000 y 80000 pesos
Debe ser el mismo para pink, amargo y leche
Si no se encuentra en el texto devuelve 0

Reglas para precio_caja_90g:
Precio de las cajas de productos de 90 gramos
Si no se encuentra en el texto devuelve 0

Reglas para precio_caja_free:
Precio de las cajas de Franuí Chocolate Free
Si no se encuentra en el texto devuelve 0

Reglas para isDelivery:
Si el pedido es para retiro en sucursal devolver false
Si no se menciona retiro explícitamente devolver true
Ejemplos de retiro:
- te quiero hacer un pedido para retirar este viernes
- pedido con retiro
En caso de duda devolver true por defecto

IMPORTANTE: Devuelve EXACTAMENTE este formato JSON sin modificar las claves ni la estructura:
{
    "Razon_social": "valor o null",
    "Direccion_despacho": "valor o null",
    "Direcciones_encontradas": ["direccion1", "direccion2"],
    "Comuna": "valor o null",
    "Rut": "valor o null",
    "Pedido_Cantidad_Pink": 0,
    "Pedido_Cantidad_Amargo": 0,
    "Pedido_Cantidad_Leche": 0,
    "Pedido_Cantidad_Free": 0,
    "Pedido_Cantidad_Pink_90g": 0,
    "Pedido_Cantidad_Amargo_90g": 0,
    "Pedido_Cantidad_Leche_90g": 0,
    "Pedido_PrecioTotal_Pink": 0,
    "Pedido_PrecioTotal_Amargo": 0,
    "Pedido_PrecioTotal_Leche": 0,
    "Pedido_PrecioTotal_Free": 0,
    "Pedido_PrecioTotal_Pink_90g": 0,
    "Pedido_PrecioTotal_Amargo_90g": 0,
    "Pedido_PrecioTotal_Leche_90g": 0,
    "Orden_de_Compra": "valor o null",
    "Monto": 0,
    "Iva": 0,
    "Total": 0,
    "Sender_Email": "valor o vacío",
    "precio_caja": 0,
    "precio_caja_90g": 0,
    "precio_caja_free": 0,
    "URL_ADDRESS": "valor codificado",
    "PaymentMethod": { "method": "", "paymentsDays": "" },
    "isDelivery": true
}
`

        console.log("userPrompt", userPrompt);

        const response = await client.chat.completions.create({
            model: 'gpt-4o-mini',
            messages: [
                { role: 'system', content: systemPrompt },
                { role: 'user', content: userPrompt },
            ]
        });

        const jsonResponse = response.choices[0].message.content.trim();
        // Fix numbers with thousand separators (e.g., 1.098.240 -> 1098240, 208.665.60 -> 208665.60)
        const sanitizedOutput = jsonResponse
            .replace(/```json|```/g, '')
            .replace(/\n/g, '')
            .replace(/\\/g, '')
            .replace(/":\s*(\d{1,3}(?:\.\d{3})+(?:\.\d{1,2})?)\s*([,\}\]])/g, (match, num, ending) => {
                const parts = num.split('.');
                // If the last part has 1-2 digits, treat it as decimal
                if (parts.length > 1 && parts[parts.length - 1].length <= 2) {
                    const decimal = parts.pop();
                    return '": ' + parts.join('') + '.' + decimal + ending;
                }
                // Otherwise, all dots are thousand separators
                return '": ' + num.replace(/\./g, '') + ending;
            });
        console.log("***********************************************SANITIZED OUTPUT *************************************************");
        console.log({ sanitizedOutput });
        
        let parsedOutput;
        let validJson;
        try {
            parsedOutput = JSON.parse(sanitizedOutput);

            if (Array.isArray(parsedOutput)) {
                if (parsedOutput.length === 0) {
                    throw new Error('GPT returned an empty array');
                }
                console.log('GPT response is an ARRAY. Using first element as primary match.');
                validJson = parsedOutput[0];
                // Optionally keep the full array for further processing or logging
                // const fullMatches = parsedOutput;
            } else {
                console.log('GPT response is an OBJECT. Using it as primary match.');
                validJson = parsedOutput;
            }
        } catch (e) {
            throw new Error('Invalid JSON returned from GPT: ' + e.message);
        }

        console.log("***********************************************VALID JSON *************************************************");
        console.log({ validJson });

        const keyLogisticsData = keyLogistics && typeof keyLogistics === 'object' ? keyLogistics : null;
        const rappiTurboData = rappiTurbo && typeof rappiTurbo === 'object' ? rappiTurbo : null;
        if (keyLogisticsData?.rut) {
            validJson.Rut = keyLogisticsData.rut;
        }
        if (keyLogisticsData) {
            const normalizedRut = validJson.Rut ? normalizeRut(validJson.Rut) : '';
            if (normalizedRut && KEY_LOGISTICS_BLOCKED_RUTS.has(normalizedRut)) {
                validJson.Rut = null;
            }
        }

        if (source === 'gmail' && String(sender || '').toLowerCase() === PEDIDOS_YA_SENDER) {
            const extractedOrderNumber = extractPedidosYaOrderNumber({
                attachmentFilename,
                emailAttached,
                emailSubject,
                emailBody
            });
            if (extractedOrderNumber) {
                validJson.Orden_de_Compra = extractedOrderNumber;
            }
        }

        if (rappiTurboData?.rut) {
            validJson.Rut = rappiTurboData.rut;
        }
        if (rappiTurboData?.ocNumber) {
            validJson.Orden_de_Compra = rappiTurboData.ocNumber;
        }
        if (rappiTurboData?.dispatchAddress) {
            validJson.Direccion_despacho = rappiTurboData.dispatchAddress;
            const extractedAddresses = Array.isArray(validJson.Direcciones_encontradas)
                ? validJson.Direcciones_encontradas
                : [];
            const parserAddresses = Array.isArray(rappiTurboData.direccionesEncontradas)
                ? rappiTurboData.direccionesEncontradas
                : [];
            validJson.Direcciones_encontradas = Array.from(
                new Set([rappiTurboData.dispatchAddress, ...extractedAddresses, ...parserAddresses].filter(Boolean))
            );
        }
        if (rappiTurboData?.totals) {
            if (Number.isFinite(rappiTurboData.totals.subtotal)) {
                validJson.Monto = rappiTurboData.totals.subtotal;
            }
            if (Number.isFinite(rappiTurboData.totals.iva)) {
                validJson.Iva = rappiTurboData.totals.iva;
            }
            if (Number.isFinite(rappiTurboData.totals.total)) {
                validJson.Total = rappiTurboData.totals.total;
            }
        }
        if (rappiTurboData?.priceTotals) {
            validJson.Pedido_PrecioTotal_Pink = rappiTurboData.priceTotals.Pedido_PrecioTotal_Pink || 0;
            validJson.Pedido_PrecioTotal_Amargo = rappiTurboData.priceTotals.Pedido_PrecioTotal_Amargo || 0;
            validJson.Pedido_PrecioTotal_Leche = rappiTurboData.priceTotals.Pedido_PrecioTotal_Leche || 0;
            validJson.Pedido_PrecioTotal_Free = rappiTurboData.priceTotals.Pedido_PrecioTotal_Free || 0;
            validJson.Pedido_PrecioTotal_Pink_90g = rappiTurboData.priceTotals.Pedido_PrecioTotal_Pink_90g || 0;
            validJson.Pedido_PrecioTotal_Amargo_90g = rappiTurboData.priceTotals.Pedido_PrecioTotal_Amargo_90g || 0;
            validJson.Pedido_PrecioTotal_Leche_90g = rappiTurboData.priceTotals.Pedido_PrecioTotal_Leche_90g || 0;
        }

        const isKeyLogisticsGmail =
            source === 'gmail' &&
            String(sender || '').toLowerCase() === KEY_LOGISTICS_SENDER;
        const isRappiTurboGmail =
            source === 'gmail' &&
            (String(sender || '').toLowerCase().endsWith('@rappi.com') || String(sender || '').toLowerCase().endsWith('@rappi.cl'));
        const keyLogisticsFixedClientData =
            isKeyLogisticsGmail && keyLogisticsData?.clientId
                ? buildKeyLogisticsClientData(
                    keyLogisticsData.clientId,
                    emailDate,
                    validJson?.precio_caja
                )
                : null;

        if (keyLogisticsFixedClientData) {
            validJson.Rut = KEY_LOGISTICS_FIXED_RUT;
        }

        const missingRut = (
            !validJson.Rut
            || validJson.Rut == 'null'
            || validJson.Rut == ''
            || validJson.Rut == 'undefined'
            || validJson.Rut == null
            || validJson.Rut == undefined
            || validJson.Rut == 'N/A'
        );
        const isManualPortalPedidosYa = (
            source === 'manual_portal'
            && String(sender || '').toLowerCase() === PEDIDOS_YA_SENDER
        );
        if (missingRut && isManualPortalPedidosYa) {
            const targetOrderNumber = String(
                parsedRequestBody?.manualOc?.targetOrderNumber
                || parsedRequestBody?.targetOrderNumber
                || ''
            ).trim().toUpperCase();
            const parsedOrders = extractPedidosYaOrdersFromAttachment(parsedRequestBody);
            const selectedOrder = targetOrderNumber
                ? (parsedOrders.find((order) => String(order?.orderNumber || '').trim().toUpperCase() === targetOrderNumber) || null)
                : (parsedOrders[0] || null);

            const peyaAddressCandidates = [];
            const pushAddressCandidate = (value) => {
                const address = String(value || '').trim();
                if (!address) {
                    return;
                }
                if (!peyaAddressCandidates.includes(address)) {
                    peyaAddressCandidates.push(address);
                }
            };

            pushAddressCandidate(validJson.Direccion_despacho);
            if (Array.isArray(validJson.Direcciones_encontradas)) {
                for (const address of validJson.Direcciones_encontradas) {
                    pushAddressCandidate(address);
                }
            }
            pushAddressCandidate(selectedOrder?.storeAddress);

            const matchedClient = await findClientByAddressInCsv(peyaAddressCandidates, emailDate);
            if (matchedClient?.data?.RUT) {
                validJson.Rut = matchedClient.data.RUT;
                validJson.Direccion_despacho = validJson.Direccion_despacho || matchedClient.data['Direccion Despacho'] || null;
                validJson.Comuna = validJson.Comuna || matchedClient.data['Comuna Despacho'] || null;

                const mergedAddresses = Array.from(new Set([
                    ...(Array.isArray(validJson.Direcciones_encontradas) ? validJson.Direcciones_encontradas : []),
                    matchedClient.data['Direccion Despacho'] || '',
                    selectedOrder?.storeAddress || ''
                ].map((address) => String(address || '').trim()).filter(Boolean)));
                validJson.Direcciones_encontradas = mergedAddresses;
            }
        }

        const injectedQuantities = keyLogisticsData?.quantities || rappiTurboData?.quantities;
        const pedidosYaDeterministicQuantities =
            String(sender || '').toLowerCase() === PEDIDOS_YA_SENDER
                ? parsePedidosYaOrderQuantities(parsedRequestBody)
                : null;
        let rutIsFound = false
        if (!validJson.Rut || validJson.Rut == "null" || validJson.Rut == "" || validJson.Rut == "undefined" || validJson.Rut == null || validJson.Rut == undefined || validJson.Rut == "N/A") {
            const foundSpecialCustomer = foundSpecialCustomers(validJson.Razon_social);
            if (foundSpecialCustomer) {
                validJson.Rut = foundSpecialCustomer;
                rutIsFound = true
            }
        } else {
            rutIsFound = true
        }

        const analyzeOrderEmaiResponse = injectedQuantities
            ? { ...EMPTY_ORDER_QUANTITIES, ...injectedQuantities }
            : (pedidosYaDeterministicQuantities
                ? { ...EMPTY_ORDER_QUANTITIES, ...pedidosYaDeterministicQuantities }
                : (source === 'gmail'
                    ? await analyzeOrderEmailFromGmail(sanitizedEmailBody)
                    : await analyzeOrderEmail(sanitizedEmailBody)));
        console.log("analyzeOrderEmaiResponse", analyzeOrderEmaiResponse);
        // Productos 150g (24 unidades por caja)
        validJson.Pedido_Cantidad_Pink = analyzeOrderEmaiResponse.Pedido_Cantidad_Pink || 0;
        validJson.Pedido_Cantidad_Amargo = analyzeOrderEmaiResponse.Pedido_Cantidad_Amargo || 0;
        validJson.Pedido_Cantidad_Leche = analyzeOrderEmaiResponse.Pedido_Cantidad_Leche || 0;
        validJson.Pedido_Cantidad_Free = analyzeOrderEmaiResponse.Pedido_Cantidad_Free || 0;
        // Productos 90g (18 unidades por caja)
        validJson.Pedido_Cantidad_Pink_90g = analyzeOrderEmaiResponse.Pedido_Cantidad_Pink_90g || 0;
        validJson.Pedido_Cantidad_Amargo_90g = analyzeOrderEmaiResponse.Pedido_Cantidad_Amargo_90g || 0;
        validJson.Pedido_Cantidad_Leche_90g = analyzeOrderEmaiResponse.Pedido_Cantidad_Leche_90g || 0;

        console.log("****************************************RUT IS FOUND *************************************************");
        console.log("rutIsFound", rutIsFound);


        // if(!validJson.Rut || validJson.Rut == "null" || validJson.Rut == "" || validJson.Rut == "undefined" || validJson.Rut == null || validJson.Rut == undefined || validJson.Rut == "N/A") {
        if (rutIsFound == false) {

            Object.keys(validJson).forEach((key) => {
                if (
                    validJson[key] === null ||
                    validJson[key] === "null" ||
                    validJson[key] === undefined ||
                    validJson[key] === "undefined" ||
                    validJson[key] === ""
                ) {
                    validJson[key] = `[${validJson[key]}] [${key}]`;
                }
            });
            return res.status(400).json({
                success: false,
                error: 'No se encuentra RUT en el correo',
                data: validJson,
                requestBody: req.body,
                executionDate: moment().format('DD-MM-YYYY HH:mm:ss'),
                OC_date: moment().format('DD-MM-YYYY')
            });
        }

        if (keyLogisticsFixedClientData) {
            const fixedData = keyLogisticsFixedClientData.data;
            validJson.Direccion_despacho = fixedData['Direccion Despacho'];
            validJson.Comuna = fixedData['Comuna Despacho'];
            validJson.Rut = fixedData.RUT;

            const fixedAddresses = [
                fixedData['Direccion Despacho'],
                fixedData['Direccion Facturación']
            ].filter(Boolean);
            const extractedAddresses = Array.isArray(validJson.Direcciones_encontradas)
                ? validJson.Direcciones_encontradas
                : [];
            validJson.Direcciones_encontradas = Array.from(
                new Set([...extractedAddresses, ...fixedAddresses])
            );

            const regionDespacho = fixedData['Region Despacho'];
            const regionNormalized = String(regionDespacho || '').toLowerCase().trim();
            if (regionNormalized === "santiago") {
                keyLogisticsFixedClientData.data['region'] = "RM";
            } else if (regionNormalized === "ohiggins" || regionNormalized === "o'higgins") {
                keyLogisticsFixedClientData.data['region'] = "VI";
            } else if (regionNormalized === "valparaíso" || regionNormalized === "valparaiso") {
                keyLogisticsFixedClientData.data['region'] = "V";
            } else {
                keyLogisticsFixedClientData.data['region'] = "";
            }

            let formattedEmailDate = "";
            if (moment(emailDate, moment.ISO_8601, true).isValid()) {
                formattedEmailDate = moment(emailDate).tz('America/Santiago').format('DD-MM-YYYY HH:mm:ss');
            }

            const merged = {
                "EmailData": { ...validJson },
                "ClientData": { ...keyLogisticsFixedClientData },
                "executionDate": moment().format('DD-MM-YYYY HH:mm:ss'),
                "OC_date": moment().format('DD-MM-YYYY'),
                "emailDate": moment(emailDate, moment.ISO_8601, true).isValid() ? formattedEmailDate : emailDate,
                "hasMatch": true
            };

            const deliveryReservation = await reserveRmDeliveryCapacity({
                emailData: validJson,
                clientData: keyLogisticsFixedClientData,
                emailContext: {
                    emailSubject,
                    emailDate,
                    sender,
                    source,
                    attachmentFilename
                }
            });

            applyDeliveryReservationToMergedResponse(merged, deliveryReservation);

            res.status(200).json({
                merged,
                deliveryReservation
            });
            return;
        }

        // Construir lista de direcciones a probar (primero la principal, luego las alternativas)
        const direccionesAProbar = [];
        
        // Agregar dirección principal si existe
        if (validJson.Direccion_despacho && validJson.Direccion_despacho !== 'null' && validJson.Direccion_despacho !== null) {
            direccionesAProbar.push(validJson.Direccion_despacho);
        }
        
        // Agregar direcciones encontradas que no sean la principal
        if (Array.isArray(validJson.Direcciones_encontradas)) {
            validJson.Direcciones_encontradas.forEach(dir => {
                if (dir && dir !== validJson.Direccion_despacho && !direccionesAProbar.includes(dir)) {
                    direccionesAProbar.push(dir);
                }
            });
        }
        
        console.log("****************************************DIRECCIONES A PROBAR*************************************************");
        console.log("direccionesAProbar", direccionesAProbar);
        
        // Intentar con cada dirección hasta encontrar una coincidencia válida
        let clientData = null;
        let direccionUsada = null;
        
        for (const direccion of direccionesAProbar) {
            console.log(`Probando dirección: ${direccion}`);
            const resultado = await readCSV_private(
                validJson.Rut,
                direccion,
                validJson.precio_caja,
                validJson.isDelivery,
                emailDate,
                { useRappiDeliverySchedule: isRappiTurboGmail }
            );
            
            // Verificar si encontramos datos válidos (no array vacío y tiene Region Despacho)
            const regionDespachoTemp = resultado?.data?.['Region Despacho'];
            const esValido = resultado.data && 
                             !Array.isArray(resultado.data) && 
                             typeof regionDespachoTemp === 'string' && 
                             regionDespachoTemp.trim() !== '';
            
            if (esValido) {
                clientData = resultado;
                direccionUsada = direccion;
                console.log(`Coincidencia encontrada con direccion: ${direccion}`);
                break;
            } else {
                console.log(`Sin coincidencia para direccion: ${direccion}`);
            }
        }
        
        // Si no encontramos nada con ninguna dirección, usar el resultado del último intento o hacer uno con la principal
        if (!clientData) {
            clientData = await readCSV_private(
                validJson.Rut,
                validJson.Direccion_despacho,
                validJson.precio_caja,
                validJson.isDelivery,
                emailDate,
                { useRappiDeliverySchedule: isRappiTurboGmail }
            );
        }
        
        console.log("clientData", clientData);
        console.log("clientData Region Despacho", clientData.data?.['Region Despacho']);
        console.log("Direccion usada para match:", direccionUsada);
        console.log("{}{}{}{}{}{}{}{}{}{}{}{}{}{}}{{}}{{}}{}{}{}{}{}{}{}{}{");
        
        // Verificar si clientData.data existe, no es array, y tiene 'Region Despacho' como string válido
        const regionDespacho = clientData?.data?.['Region Despacho'];
        const isValidClientData = clientData.data && 
                                   !Array.isArray(clientData.data) && 
                                   typeof regionDespacho === 'string' && 
                                   regionDespacho.trim() !== '';
        
        if (!isValidClientData) {
            let formattedEmailDate = "";
            if (moment(emailDate, moment.ISO_8601, true).isValid()) {
                formattedEmailDate = moment(emailDate).tz('America/Santiago').format('DD-MM-YYYY HH:mm:ss');
            }

            const mergedNoMatch = {
                "EmailData": { ...validJson },
                "ClientData": { ...clientData },
                "executionDate": moment().format('DD-MM-YYYY HH:mm:ss'),
                "OC_date": moment().format('DD-MM-YYYY'),
                "emailDate": moment(emailDate, moment.ISO_8601, true).isValid() ? formattedEmailDate : emailDate,
                "hasMatch": false
            };

            // Si no hay datos del cliente válidos, retornar error con info de direcciones probadas
            return res.status(400).json({
                success: false,
                error: 'No se encontró coincidencia de dirección en la base de clientes',
                direccionesProbadas: direccionesAProbar,
                cantidadDireccionesProbadas: direccionesAProbar.length,
                data: validJson,
                clientData: clientData,
                merged: mergedNoMatch,
                requestBody: req.body,
                executionDate: moment().format('DD-MM-YYYY HH:mm:ss'),
                OC_date: moment().format('DD-MM-YYYY')
            });
        }
        
        // Si usamos una dirección alternativa, actualizar validJson para reflejar la correcta
        if (direccionUsada && direccionUsada !== validJson.Direccion_despacho) {
            console.log(`Actualizando Direccion_despacho de "${validJson.Direccion_despacho}" a "${direccionUsada}"`);
            validJson.Direccion_despacho_original = validJson.Direccion_despacho;
            validJson.Direccion_despacho = direccionUsada;
        }
        
        // Ahora es seguro usar toLowerCase() porque ya validamos que es string no vacío
        const regionNormalized = regionDespacho.toLowerCase().trim();
        
        if (regionNormalized === "santiago") {
            clientData.data['region'] = "RM";
        } else if (regionNormalized === "ohiggins" || regionNormalized === "o'higgins") {
            clientData.data['region'] = "VI";
        } else if (regionNormalized === "valparaíso" || regionNormalized === "valparaiso") {
            clientData.data['region'] = "V";
        } else {
            clientData.data['region'] = "";
        }
        console.log("clientData.data con region", clientData.data.region);

        let formattedEmailDate = "";
        if (moment(emailDate, moment.ISO_8601, true).isValid()) {
            formattedEmailDate = moment(emailDate).tz('America/Santiago').format('DD-MM-YYYY HH:mm:ss');
        }

        const merged = {
            "EmailData": { ...validJson },
            "ClientData": { ...clientData },
            "executionDate": moment().format('DD-MM-YYYY HH:mm:ss'),
            "OC_date": moment().format('DD-MM-YYYY'),
            "emailDate": moment(emailDate, moment.ISO_8601, true).isValid() ? formattedEmailDate : emailDate,
            "hasMatch": true,
            "order_origin": (() => {
                if (/ENX_ORD_\d+/.test(String(emailSubject || ''))) return 'Form';
                if (source === 'manual_portal') return 'OC Loader';
                return 'Email';
            })(),
        };

        const deliveryReservation = await reserveRmDeliveryCapacity({
            emailData: validJson,
            clientData,
            emailContext: {
                emailSubject,
                emailDate,
                sender,
                source,
                attachmentFilename
            }
        });

        applyDeliveryReservationToMergedResponse(merged, deliveryReservation);

        res.status(200).json({
            merged,
            deliveryReservation
        });
        return;

    } catch (error) {
        console.log(error);

        if (error?.code && error?.message) {
            return res.status(error.code).json({
                success: false,
                error: error.error || 'Error en procesamiento de despacho',
                details: error.message,
                requestBody: req.body,
                executionDate: moment().format('DD-MM-YYYY HH:mm:ss'),
                OC_date: moment().format('DD-MM-YYYY')
            });
        }

        const response = {
            "Razon_social": "[null]  Razon_social",
            "Direccion_despacho": "[null]  Direccion_despacho",
            "Comuna": "[null]  Comuna",
            "Rut": "[null]  Rut",
            "Pedido_Cantidad_Pink": "[null]  Pedido_Cantidad_Pink",
            "Pedido_Cantidad_Amargo": "[null]  Pedido_Cantidad_Amargo",
            "Pedido_Cantidad_Leche": "[null]  Pedido_Cantidad_Leche",
            "Pedido_Cantidad_Free": "[null]  Pedido_Cantidad_Free",
            "Pedido_Cantidad_Pink_90g": "[null]  Pedido_Cantidad_Pink_90g",
            "Pedido_Cantidad_Amargo_90g": "[null]  Pedido_Cantidad_Amargo_90g",
            "Pedido_Cantidad_Leche_90g": "[null]  Pedido_Cantidad_Leche_90g",
            "Pedido_PrecioTotal_Pink": "[null]  Pedido_PrecioTotal_Pink",
            "Pedido_PrecioTotal_Amargo": "[null]  Pedido_PrecioTotal_Amargo",
            "Pedido_PrecioTotal_Leche": "[null]  Pedido_PrecioTotal_Leche",
            "Pedido_PrecioTotal_Free": "[null]  Pedido_PrecioTotal_Free",
            "Pedido_PrecioTotal_Pink_90g": "[null]  Pedido_PrecioTotal_Pink_90g",
            "Pedido_PrecioTotal_Amargo_90g": "[null]  Pedido_PrecioTotal_Amargo_90g",
            "Pedido_PrecioTotal_Leche_90g": "[null]  Pedido_PrecioTotal_Leche_90g",
            "Orden_de_Compra": "[null]  Orden_de_Compra",
            "Monto neto": "[null]  Monto",
            "Iva": "[null]  Iva",
            "Total": "[null]  Total",
            "Sender_Email": "[null]  Sender_Email",
            "precio_caja": "[null]  precio_caja",
            "precio_caja_90g": "[null]  precio_caja_90g",
            "precio_caja_free": "[null]  precio_caja_free",
            "URL_ADDRESS": "[null]  URL_ADDRESS",
            "PaymentMethod": { "method": "", "paymentsDays": "" },
            "isDelivery": true
        }

        res.status(400).json({
            success: false,
            error: 'No se ha podido procesar el correo',
            requestBody: req.body,
            data: response,
            executionDate: moment().format('DD-MM-YYYY HH:mm:ss'),
            OC_date: moment().format('DD-MM-YYYY')
        });
    }
}

function sanitizeFilename(name) {
    const base = path.basename(name || 'attachment');
    return base.replace(/[^a-zA-Z0-9._-]/g, '_');
}

async function saveTempFiles(messageId, filename, buffer, text) {
    const tempDir = path.resolve(process.cwd(), 'temp');
    await fs.promises.mkdir(tempDir, { recursive: true });

    const safeName = sanitizeFilename(filename);
    const baseName = `${messageId}_${safeName}`;
    const attachmentPath = path.join(tempDir, baseName);
    const textPath = path.join(tempDir, `${baseName}.txt`);

    await fs.promises.writeFile(attachmentPath, buffer);
    await fs.promises.writeFile(textPath, text, 'utf8');

    return { attachmentPath, textPath };
}

function excelBufferToText(buffer) {
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    const maxRows = Number.parseInt(process.env.GMAIL_EXCEL_MAX_ROWS || '200', 10);
    const maxCols = Number.parseInt(process.env.GMAIL_EXCEL_MAX_COLS || '30', 10);

    const sections = workbook.SheetNames.map((sheetName) => {
        const sheet = workbook.Sheets[sheetName];
        const rows = XLSX.utils.sheet_to_json(sheet, {
            header: 1,
            defval: '',
            blankrows: false
        });

        const trimmedRows = rows
            .map((row) => row.slice(0, maxCols).map((cell) => String(cell).replace(/\s+/g, ' ').trim()))
            .filter((row) => row.some((cell) => cell !== ''))
            .slice(0, maxRows);

        const content = trimmedRows.map((row) => row.join('\t')).join('\n');
        return `Sheet: ${sheetName}\n${content}`;
    });

    return sections.join('\n\n');
}

async function pdfBufferToText(buffer) {
    const data = await pdfParse(buffer);
    return data.text || '';
}

async function runReadEmailBodyPayload(payload) {
    const requestBody = JSON.stringify(payload);

    return new Promise((resolve, reject) => {
        const fakeReq = { body: requestBody };
        const fakeRes = {
            statusCode: 200,
            status(code) {
                this.statusCode = code;
                return this;
            },
            json(body) {
                resolve({ status: this.statusCode, body });
            }
        };

        readEmailBody(fakeReq, fakeRes).catch(reject);
    });
}

function resolveManualOcBatchUi(status) {
    switch (status) {
        case 'ready':
            return {
                color: 'green',
                legend: 'Listo para procesar',
                tooltip: 'Archivo valido y sin duplicados detectados.'
            };
        case 'duplicate_batch':
            return {
                color: 'yellow',
                legend: 'Duplicado en tanda',
                tooltip: 'Esta OC se repite en la misma tanda. Se mantiene el archivo preferido (Excel).'
            };
        case 'duplicate_backend':
            return {
                color: 'orange',
                legend: 'OC ya procesada',
                tooltip: 'Esta OC ya existe como procesada en el backend.'
            };
        case 'conflict':
            return {
                color: 'red',
                legend: 'Conflictivo',
                tooltip: 'Misma OC con datos distintos entre archivos. Requiere resolucion manual en UI.'
            };
        case 'missing_oc':
            return {
                color: 'gray',
                legend: 'Sin OC detectada',
                tooltip: 'No se pudo detectar numero de OC desde el contenido del archivo.'
            };
        case 'address_not_found':
            return {
                color: 'red',
                legend: 'Direccion no encontrada',
                tooltip: 'Se analizo la OC, pero la direccion no existe en la base de clientes.'
            };
        default:
            return {
                color: 'red',
                legend: 'Error',
                tooltip: 'No se pudo analizar este archivo.'
            };
    }
}

function isManualOcDedupStatus(status) {
    const safeStatus = String(status || '').trim();
    return safeStatus === 'duplicate_batch' || safeStatus === 'duplicate_backend';
}

function resolveManualOcBatchBlockCategory(status) {
    const safeStatus = String(status || '').trim();
    if (safeStatus === 'ready') {
        return 'none';
    }
    if (isManualOcDedupStatus(safeStatus)) {
        return 'dedup';
    }
    if (safeStatus === 'address_not_found') {
        return 'address';
    }
    if (safeStatus === 'conflict') {
        return 'conflict';
    }
    if (safeStatus === 'missing_oc') {
        return 'missing_oc';
    }
    return 'error';
}

function formatManualOcBackendDuplicate(record) {
    if (!record) {
        return null;
    }
    return {
        exists: true,
        manualOcId: record.manualOcId || null,
        status: record.status || null,
        createdAt: record.createdAt || null,
        updatedAt: record.updatedAt || null
    };
}

function collectManualOcAddressCandidates(...values) {
    return Array.from(new Set(
        values
            .flat()
            .map((value) => String(value || '').trim())
            .filter(Boolean)
    ));
}

async function runManualOcBatchAddressPrecheck(entries = []) {
    const lookupCache = new Map();

    for (const entry of entries) {
        const defaultState = {
            checked: false,
            ok: null,
            code: 'not_checked',
            reason: 'No fue necesario validar direccion en esta fila',
            attemptedAddresses: []
        };

        if (!entry) {
            continue;
        }

        if (!entry.detectedOrderNumber || entry.status === 'error' || entry.status === 'missing_oc') {
            entry.dispatchAddressPrecheck = defaultState;
            continue;
        }

        const addressCandidates = collectManualOcAddressCandidates(entry.addressCandidates || []);
        if (addressCandidates.length === 0) {
            entry.dispatchAddressPrecheck = {
                checked: false,
                ok: null,
                code: 'dispatch_address_not_detected',
                reason: 'No se detecto direccion de despacho en el archivo',
                attemptedAddresses: []
            };
            continue;
        }

        const referenceDate = parseManualDateCandidate(entry.detectedDate) || moment().format('YYYY-MM-DD');
        const lookupKey = `${referenceDate}|${addressCandidates.join('|').toLowerCase()}`;

        let matchedClient = null;
        let lookupError = null;
        if (lookupCache.has(lookupKey)) {
            const cachedLookup = lookupCache.get(lookupKey) || {};
            matchedClient = cachedLookup.matchedClient || null;
            lookupError = cachedLookup.lookupError || null;
        } else {
            try {
                matchedClient = await findClientByAddressInCsv(
                    addressCandidates,
                    `${referenceDate}T12:00:00-03:00`
                );
                lookupCache.set(lookupKey, {
                    matchedClient: matchedClient || null,
                    lookupError: null
                });
            } catch (error) {
                lookupError = error;
                lookupCache.set(lookupKey, {
                    matchedClient: null,
                    lookupError: error
                });
            }
        }

        if (lookupError) {
            entry.dispatchAddressPrecheck = {
                checked: true,
                ok: null,
                code: 'address_precheck_error',
                reason: `No se pudo validar direccion en esta fila: ${lookupError?.message || lookupError}`,
                attemptedAddresses: addressCandidates
            };
            continue;
        }

        if (matchedClient?.data) {
            entry.dispatchAddressPrecheck = {
                checked: true,
                ok: true,
                code: 'ok',
                reason: 'Direccion validada en base de clientes',
                attemptedAddresses: addressCandidates,
                requestedAddress: matchedClient.requestedAddress || null,
                matchedAddress: String(
                    matchedClient?.data?.['Direccion Despacho']
                    || matchedClient?.data?.['Direccion Despacho']
                    || ''
                ).trim() || null,
                score: Number.isFinite(Number(matchedClient.score))
                    ? Math.round(Number(matchedClient.score) * 1000) / 1000
                    : null
            };
            continue;
        }

        entry.dispatchAddressPrecheck = {
            checked: true,
            ok: false,
            code: 'address_not_found_in_customer_db',
            reason: 'No se encontro la direccion en la base de clientes',
            attemptedAddresses: addressCandidates
        };
    }
}

async function analyzeManualOcBatchFile({
    file,
    index
}) {
    const fileName = String(file?.fileName || '').trim();
    const mimeType = String(file?.mimeType || '').trim();
    const fileSize = Number(file?.fileSize || 0);
    const fileBase64 = file?.fileBase64;
    const clientFileId = String(file?.clientFileId || file?.id || '').trim() || null;

    if (!fileName || !fileBase64) {
        return [{
            index,
            clientFileId,
            fileName,
            mimeType,
            fileSize,
            fileType: null,
            detectedOrderNumber: null,
            status: 'error',
            statusReason: 'missing_file_payload',
            recommendedAction: 'revisar_archivo',
            ui: resolveManualOcBatchUi('error'),
            error: 'fileName y fileBase64 son requeridos por archivo'
        }];
    }

    try {
        const fileBuffer = decodeFileBase64ToBuffer(fileBase64);
        const fileSha256 = createHash('sha256').update(fileBuffer).digest('hex');
        const extractedFile = await extractManualOcTextFromFile({
            fileName,
            mimeType,
            fileBuffer
        });

        const excelAnalysis = extractedFile?.excelPreview?.analysis || null;
        const legacyPdfAnalysis = extractedFile.fileType === 'pdf'
            ? extractPeyaPdfAnalysis(extractedFile.text)
            : null;

        const attachmentPayload = buildManualOcAttachmentPayload({
            fileName,
            mimeType,
            text: extractedFile.text
        });
        const extractedOrders = extractPedidosYaOrdersFromAttachment(attachmentPayload)
            .filter((order) => normalizeManualOcOrderNumber(order?.orderNumber));

        if (extractedOrders.length > 0) {
            return extractedOrders.map((order, subIndex) => {
                const detectedOrderNumber = normalizeManualOcOrderNumber(order?.orderNumber);
                const targetOrderNumber = detectedOrderNumber || null;
                const orderPayload = buildManualOcAttachmentPayload({
                    fileName,
                    mimeType,
                    text: String(order?.orderText || extractedFile.text || ''),
                    targetOrderNumber
                });
                const quantities = order?.quantities
                    || parsePedidosYaOrderQuantities(JSON.stringify(orderPayload))
                    || { ...EMPTY_ORDER_QUANTITIES };
                const syntheticAnalysis = buildManualOcSyntheticAnalysisFromExtractedOrder(order);
                const addressCandidates = collectManualOcAddressCandidates(
                    order?.storeAddress,
                    syntheticAnalysis?.metadata?.direccionEntrega
                );
                const detectedDateInfo = detectOcDateFromText(String(order?.orderText || extractedFile.text || ''));
                const detectedDate = detectedDateInfo?.date
                    || parseManualDateCandidate(order?.orderDate)
                    || syntheticAnalysis?.dates?.fechaEmision
                    || null;
                const comparableSnapshot = buildManualOcComparableSnapshot({
                    detectedOrderNumber,
                    quantities,
                    excelAnalysis: extractedFile.fileType === 'excel' ? syntheticAnalysis : excelAnalysis,
                    pdfAnalysis: extractedFile.fileType === 'pdf' ? (legacyPdfAnalysis || syntheticAnalysis) : null
                });
                const stableOrderSuffix = targetOrderNumber || `ORD-${subIndex + 1}`;
                const expandedClientFileId = clientFileId && extractedOrders.length > 1
                    ? `${clientFileId}::${stableOrderSuffix}`
                    : clientFileId;
                const expandedFileName = extractedOrders.length > 1
                    ? `${fileName} [${stableOrderSuffix}]`
                    : fileName;

                return {
                    index: index * 1000 + subIndex,
                    clientFileId: expandedClientFileId,
                    originClientFileId: clientFileId,
                    fileName: expandedFileName,
                    sourceFileName: fileName,
                    mimeType,
                    fileSize,
                    fileSha256,
                    fileType: extractedFile.fileType,
                    targetOrderNumber,
                    detectedOrderNumber,
                    detectedDate,
                    quantities,
                    itemCount: comparableSnapshot?.itemStats?.count || order?.itemCount || 0,
                    comparableSnapshot,
                    excelAnalysis: extractedFile.fileType === 'excel' ? syntheticAnalysis : excelAnalysis,
                    pdfAnalysis: extractedFile.fileType === 'pdf' ? (legacyPdfAnalysis || syntheticAnalysis) : null,
                    scopedOrderText: String(order?.orderText || extractedFile.text || ''),
                    addressCandidates,
                    backendDuplicate: null,
                    status: detectedOrderNumber ? 'pending' : 'missing_oc',
                    statusReason: detectedOrderNumber ? 'pending_group_resolution' : 'missing_order_number',
                    recommendedAction: detectedOrderNumber ? 'evaluar_deduplicacion' : 'resolver_manual_oc',
                    ui: resolveManualOcBatchUi(detectedOrderNumber ? 'ready' : 'missing_oc'),
                    error: null
                };
            });
        }

        const detectedOrderNumber = (
            extractManualOcOrderNumber({ fileName, text: extractedFile.text })
            || normalizeManualOcOrderNumber(excelAnalysis?.purchaseOrderNumber)
            || normalizeManualOcOrderNumber(legacyPdfAnalysis?.purchaseOrderNumber)
        );
        const quantitiesPayload = buildManualOcPayloadForQuantities({
            fileName,
            mimeType,
            text: extractedFile.text
        });
        const quantities = parsePedidosYaOrderQuantities(quantitiesPayload)
            || { ...EMPTY_ORDER_QUANTITIES };
        const comparableSnapshot = buildManualOcComparableSnapshot({
            detectedOrderNumber,
            quantities,
            excelAnalysis,
            pdfAnalysis: legacyPdfAnalysis
        });
        const detectedDateInfo = detectOcDateFromText(extractedFile.text);
        const detectedDate = detectedDateInfo?.date
            || excelAnalysis?.dates?.fechaEmision
            || legacyPdfAnalysis?.dates?.fechaEmision
            || null;
        const addressCandidates = collectManualOcAddressCandidates(
            excelAnalysis?.metadata?.direccionEntrega,
            legacyPdfAnalysis?.metadata?.direccionEntrega
        );

        return [{
            index,
            clientFileId,
            originClientFileId: clientFileId,
            fileName,
            sourceFileName: fileName,
            mimeType,
            fileSize,
            fileSha256,
            fileType: extractedFile.fileType,
            targetOrderNumber: detectedOrderNumber || null,
            detectedOrderNumber,
            detectedDate,
            quantities,
            itemCount: comparableSnapshot?.itemStats?.count || 0,
            comparableSnapshot,
            excelAnalysis,
            pdfAnalysis: legacyPdfAnalysis,
            scopedOrderText: extractedFile.text,
            addressCandidates,
            backendDuplicate: null,
            status: detectedOrderNumber ? 'pending' : 'missing_oc',
            statusReason: detectedOrderNumber ? 'pending_group_resolution' : 'missing_order_number',
            recommendedAction: detectedOrderNumber ? 'evaluar_deduplicacion' : 'resolver_manual_oc',
            ui: resolveManualOcBatchUi(detectedOrderNumber ? 'ready' : 'missing_oc'),
            error: null
        }];
    } catch (error) {
        return [{
            index,
            clientFileId,
            fileName,
            mimeType,
            fileSize,
            fileType: null,
            detectedOrderNumber: null,
            status: 'error',
            statusReason: 'file_read_failed',
            recommendedAction: 'revisar_archivo',
            ui: resolveManualOcBatchUi('error'),
            error: error?.message || String(error)
        }];
    }
}

function finalizeManualOcBatchDecisions(entries = []) {
    const groupedByOrder = new Map();
    const preferredByOrder = new Map();

    for (const entry of entries) {
        if (entry.status === 'error' || entry.status === 'missing_oc' || !entry.detectedOrderNumber) {
            continue;
        }
        const key = entry.detectedOrderNumber;
        if (!groupedByOrder.has(key)) {
            groupedByOrder.set(key, []);
        }
        groupedByOrder.get(key).push(entry);
    }

    for (const [orderNumber, group] of groupedByOrder.entries()) {
        group.sort((a, b) => a.index - b.index);
        const excelCandidates = group.filter((entry) => entry.fileType === 'excel');
        const preferredGroup = excelCandidates.length > 0 ? excelCandidates : group;
        const preferred = preferredGroup[0];
        preferredByOrder.set(orderNumber, preferred);

        const preferredConflict = preferredGroup.some((entry) => (
            !areManualOcSnapshotsEquivalent(entry.comparableSnapshot, preferred.comparableSnapshot)
        ));

        if (preferredConflict) {
            for (const entry of group) {
                entry.status = 'conflict';
                entry.statusReason = 'same_oc_different_data';
                entry.recommendedAction = 'resolver_conflicto';
                entry.isPreferredInBatch = entry.index === preferred.index;
                entry.preferredFileName = preferred.fileName;
                entry.ui = resolveManualOcBatchUi('conflict');
            }
            continue;
        }

        for (const entry of group) {
            entry.isPreferredInBatch = entry.index === preferred.index;
            entry.preferredFileName = preferred.fileName;

            if (entry.index !== preferred.index) {
                if (excelCandidates.length > 0 && entry.fileType === 'pdf') {
                    entry.status = 'duplicate_batch';
                    entry.statusReason = 'duplicate_oc_prefer_excel';
                    entry.recommendedAction = 'descartar_duplicado';
                    entry.ui = resolveManualOcBatchUi('duplicate_batch');
                    continue;
                }

                const equivalentToPreferred = areManualOcSnapshotsEquivalent(
                    entry.comparableSnapshot,
                    preferred.comparableSnapshot
                );
                if (!equivalentToPreferred) {
                    entry.status = 'conflict';
                    entry.statusReason = 'same_oc_different_data';
                    entry.recommendedAction = 'resolver_conflicto';
                    entry.ui = resolveManualOcBatchUi('conflict');
                    continue;
                }

                entry.status = 'duplicate_batch';
                entry.statusReason = preferred.fileType === 'excel' && entry.fileType === 'pdf'
                    ? 'duplicate_oc_prefer_excel'
                    : 'duplicate_oc_in_batch';
                entry.recommendedAction = 'descartar_duplicado';
                entry.ui = resolveManualOcBatchUi('duplicate_batch');
                continue;
            }

            if (entry.backendDuplicate?.exists) {
                entry.status = 'duplicate_backend';
                entry.statusReason = 'oc_already_processed_backend';
                entry.recommendedAction = 'no_procesar';
                entry.ui = resolveManualOcBatchUi('duplicate_backend');
            } else if (entry.dispatchAddressPrecheck?.checked === true && entry.dispatchAddressPrecheck?.ok === false) {
                entry.status = 'address_not_found';
                entry.statusReason = String(entry.dispatchAddressPrecheck.code || 'address_not_found_in_customer_db');
                entry.recommendedAction = 'resolver_direccion';
                entry.ui = resolveManualOcBatchUi('address_not_found');
            } else {
                entry.status = 'ready';
                entry.statusReason = 'ok';
                entry.recommendedAction = 'procesar';
                entry.ui = resolveManualOcBatchUi('ready');
            }
        }
    }

    return preferredByOrder;
}

function summarizeManualOcBatchEntries(entries = []) {
    const counts = {
        total: entries.length,
        ready: 0,
        duplicate_batch: 0,
        duplicate_backend: 0,
        conflict: 0,
        address_not_found: 0,
        missing_oc: 0,
        error: 0
    };

    for (const entry of entries) {
        if (Object.prototype.hasOwnProperty.call(counts, entry.status)) {
            counts[entry.status] += 1;
        }
    }

    return {
        ...counts,
        blocked: counts.total - counts.ready
    };
}

async function readManualOcBatchDedup(req, res) {
    try {
        const body = req.body || {};
        const sourceClientCode = String(body.sourceClientCode || '').trim().toUpperCase();
        const uploadedBy = String(body.uploadedBy || '').trim();
        const files = Array.isArray(body.files) ? body.files : [];

        if (!uploadedBy) {
            return res.status(400).json({
                success: false,
                error: 'uploadedBy es requerido',
                errorCode: 'uploaded_by_required',
                canSubmitToInvoicer: false
            });
        }

        if (files.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'files debe contener al menos un archivo',
                errorCode: 'files_required',
                canSubmitToInvoicer: false
            });
        }

        const profile = getManualOcClientProfile(sourceClientCode);
        if (!profile) {
            return res.status(400).json({
                success: false,
                error: `sourceClientCode no soportado: ${sourceClientCode}`,
                errorCode: 'source_client_code_not_supported',
                canSubmitToInvoicer: false
            });
        }

        const analyzedEntries = [];
        for (let index = 0; index < files.length; index += 1) {
            const analyzedItems = await analyzeManualOcBatchFile({
                file: files[index],
                index
            });
            if (Array.isArray(analyzedItems)) {
                analyzedEntries.push(...analyzedItems);
            }
        }

        const uniqueOrderNumbers = Array.from(new Set(
            analyzedEntries
                .map((entry) => entry.detectedOrderNumber)
                .filter(Boolean)
        ));
        const backendDuplicatesByOrder = new Map();
        const backendDedupWarnings = [];
        for (const orderNumber of uniqueOrderNumbers) {
            try {
                const backendRecord = await findLatestManualOcByDetectedOrderNumber({
                    sourceClientCode: profile.sourceClientCode,
                    detectedOrderNumber: orderNumber,
                    statuses: MANUAL_OC_PROCESSED_STATUSES
                });
                backendDuplicatesByOrder.set(orderNumber, formatManualOcBackendDuplicate(backendRecord));
            } catch (backendError) {
                backendDuplicatesByOrder.set(orderNumber, null);
                backendDedupWarnings.push(
                    `No se pudo validar dedup backend para ${orderNumber}: ${backendError?.message || backendError}`
                );
            }
        }

        for (const entry of analyzedEntries) {
            if (!entry.detectedOrderNumber) {
                continue;
            }
            entry.backendDuplicate = backendDuplicatesByOrder.get(entry.detectedOrderNumber) || null;
        }

        await runManualOcBatchAddressPrecheck(analyzedEntries);

        const preferredByOrder = finalizeManualOcBatchDecisions(analyzedEntries);
        const summary = summarizeManualOcBatchEntries(analyzedEntries);
        const results = analyzedEntries
            .sort((a, b) => a.index - b.index)
            .map((entry) => {
                const analysis = buildManualOcBatchAnalysis({
                    status: entry.status,
                    statusReason: entry.statusReason,
                    error: entry.error,
                    dispatchAddressPrecheck: entry.dispatchAddressPrecheck
                });
                const blocked = entry.status !== 'ready';
                return {
                    index: entry.index,
                    clientFileId: entry.clientFileId || null,
                    originClientFileId: entry.originClientFileId || entry.clientFileId || null,
                    fileName: entry.fileName || null,
                    sourceFileName: entry.sourceFileName || entry.fileName || null,
                    mimeType: entry.mimeType || null,
                    fileSize: Number.isFinite(entry.fileSize) ? entry.fileSize : 0,
                    fileType: entry.fileType || null,
                    targetOrderNumber: entry.targetOrderNumber || null,
                    detectedOrderNumber: entry.detectedOrderNumber || null,
                    detectedDate: entry.detectedDate || null,
                    itemCount: entry.itemCount || 0,
                    quantities: entry.quantities || { ...EMPTY_ORDER_QUANTITIES },
                    status: entry.status,
                    statusReason: entry.statusReason || null,
                    recommendedAction: entry.recommendedAction || null,
                    isPreferredInBatch: entry.isPreferredInBatch === true,
                    canSubmitToInvoicer: entry.status === 'ready',
                    blocked,
                    isDedup: isManualOcDedupStatus(entry.status),
                    blockCategory: resolveManualOcBatchBlockCategory(entry.status),
                    blockReason: blocked ? analysis.reason : null,
                    preferredFileName: entry.preferredFileName || null,
                    backendDuplicate: entry.backendDuplicate || { exists: false },
                    dispatchAddressPrecheck: entry.dispatchAddressPrecheck || null,
                    analysis,
                    ui: entry.ui || resolveManualOcBatchUi(entry.status),
                    error: entry.error || null
                };
            });

        const perOrder = Array.from(preferredByOrder.entries()).map(([orderNumber, preferredEntry]) => ({
            orderNumber,
            preferredFileName: preferredEntry?.fileName || null,
            preferredFileType: preferredEntry?.fileType || null
        }));

        return res.status(200).json({
            success: true,
            sourceClientCode: profile.sourceClientCode,
            sourceClientName: profile.sourceClientName,
            parserProfile: profile.parserProfile,
            uploadedBy,
            summary,
            rulesApplied: [
                'dedup_within_batch_by_detected_order_number',
                'dedup_against_backend_processed_records',
                'prefer_excel_over_pdf_for_same_oc',
                'mark_conflict_when_same_oc_has_different_snapshot',
                'precheck_dispatch_address_against_customer_db'
            ],
            warnings: backendDedupWarnings,
            perOrder,
            results,
            canSubmitToInvoicer: summary.blocked === 0
        });
    } catch (error) {
        console.error('Error en readManualOcBatchDedup:', error);
        return res.status(500).json({
            success: false,
            error: 'No se pudo deduplicar la tanda manual OC',
            errorCode: 'batch_dedup_unhandled_exception',
            details: error?.message || String(error),
            canSubmitToInvoicer: false
        });
    }
}

async function readManualOcExtractDate(req, res) {
    try {
        const body = req.body || {};
        const sourceClientCode = String(body.sourceClientCode || '')
            .trim()
            .toUpperCase();
        const uploadedBy = String(body.uploadedBy || '').trim();
        const fileName = String(body.fileName || '').trim();
        const mimeType = String(body.mimeType || '').trim();
        const fileSize = Number(body.fileSize || 0);
        const fileBase64 = body.fileBase64;
        const targetOrderNumberInput = normalizeManualOcOrderNumber(
            String(body.targetOrderNumber || '').trim()
        );

        if (!uploadedBy) {
            return res.status(400).json({
                success: false,
                error: 'uploadedBy es requerido',
                errorCode: 'uploaded_by_required',
                canSubmitToInvoicer: false
            });
        }

        if (!fileName || !fileBase64) {
            return res.status(400).json({
                success: false,
                error: 'fileName y fileBase64 son requeridos',
                errorCode: 'file_name_or_file_base64_required',
                canSubmitToInvoicer: false
            });
        }

        const profile = getManualOcClientProfile(sourceClientCode);
        if (!profile) {
            return res.status(400).json({
                success: false,
                error: `sourceClientCode no soportado: ${sourceClientCode}`,
                errorCode: 'source_client_code_not_supported',
                canSubmitToInvoicer: false
            });
        }

        const fileBuffer = decodeFileBase64ToBuffer(fileBase64);
        const fileSha256 = createHash('sha256').update(fileBuffer).digest('hex');

        let extractedFile;
        try {
            extractedFile = await extractManualOcTextFromFile({
                fileName,
                mimeType,
                fileBuffer
            });
        } catch (fileError) {
            return res.status(400).json({
                success: false,
                error: fileError?.message || 'No se pudo leer el archivo',
                errorCode: 'file_decode_or_extract_failed',
                canSubmitToInvoicer: false
            });
        }

        const excelAnalysis = extractedFile?.excelPreview?.analysis || null;
        const attachmentPayload = buildManualOcAttachmentPayload({
            fileName,
            mimeType,
            text: extractedFile.text,
            targetOrderNumber: targetOrderNumberInput
        });
        const parsedOrders = extractPedidosYaOrdersFromAttachment(attachmentPayload);
        const selectedOrder = targetOrderNumberInput
            ? parsedOrders.find((order) => normalizeManualOcOrderNumber(order?.orderNumber) === targetOrderNumberInput) || null
            : (parsedOrders.length === 1 ? parsedOrders[0] : null);
        if (targetOrderNumberInput && !selectedOrder) {
            return res.status(422).json({
                errorCode: 'target_order_not_found_in_file',
                success: false,
                error: `No se encontro la OC objetivo ${targetOrderNumberInput} dentro del archivo`,
                targetOrderNumber: targetOrderNumberInput,
                canSubmitToInvoicer: false
            });
        }
        const scopedOrderText = String(selectedOrder?.orderText || extractedFile.text || '');
        const syntheticAnalysis = selectedOrder
            ? buildManualOcSyntheticAnalysisFromExtractedOrder(selectedOrder)
            : null;
        const detectedDateInfoFromText = detectOcDateFromText(scopedOrderText);
        // Prefer structured date from PDF/Excel parser (more reliable than generic text scan).
        // The generic regex can misfire on concatenated sequences like "PO46920408/04/2026"
        // matching tax amounts as Excel serial dates (e.g. IVA 72094 â†’ 2097-05-19).
        const detectedDateInfo = syntheticAnalysis?.dates?.fechaEmision
            ? {
                date: syntheticAnalysis.dates.fechaEmision,
                confidence: 'high',
                method: 'structured_order_order_date'
            }
            : (detectedDateInfoFromText?.date
                ? detectedDateInfoFromText
                : { date: null, confidence: 'none', method: 'not_found' });
        const detectedOrderNumber = (
            normalizeManualOcOrderNumber(selectedOrder?.orderNumber)
            || extractManualOcOrderNumber({ fileName, text: scopedOrderText })
            || normalizeManualOcOrderNumber(excelAnalysis?.purchaseOrderNumber)
        );
        const backendDuplicateRecord = detectedOrderNumber
            ? await findLatestManualOcByDetectedOrderNumber({
                sourceClientCode: profile.sourceClientCode,
                detectedOrderNumber,
                statuses: MANUAL_OC_PROCESSED_STATUSES
            })
            : null;
        if (backendDuplicateRecord) {
            return res.status(409).json({
                success: false,
                error: 'Orden de compra ya procesada en backend',
                errorCode: 'duplicate_backend_extract_guard',
                duplicate: true,
                duplicateSource: 'backend',
                detectedOrderNumber,
                backendRecord: formatManualOcBackendDuplicate(backendDuplicateRecord),
                canSubmitToInvoicer: false
            });
        }
        const manualOcId = randomUUID();
        const warnings = [];
        if (!targetOrderNumberInput && parsedOrders.length > 1) {
            warnings.push(`El archivo contiene ${parsedOrders.length} OCs. Debes indicar targetOrderNumber para extraer una OC puntual.`);
        }

        if (!detectedDateInfo.date) {
            warnings.push('No se detecto fecha OC automaticamente, usar fecha manual.');
        }
        if (extractedFile.fileType === 'excel' && excelAnalysis && !selectedOrder && !excelAnalysis.dates?.fechaEmision) {
            warnings.push('No se detecto Fecha emision en la estructura del Excel.');
        }
        if (extractedFile.fileType === 'excel' && excelAnalysis && !selectedOrder && (excelAnalysis.itemStats?.count || 0) === 0) {
            warnings.push('No se detectaron items en el bloque de detalle del Excel.');
        }

        await createManualOcRecord({
            manualOcId,
            status: 'date_extracted',
            sourceClientCode: profile.sourceClientCode,
            sourceClientName: profile.sourceClientName,
            parserProfile: profile.parserProfile,
            syntheticSender: profile.syntheticSender,
            uploadedBy,
            fileMeta: {
                fileName,
                mimeType,
                fileSize,
                fileSha256,
                fileType: extractedFile.fileType
            },
            ocDateDetected: detectedDateInfo.date || null,
            ocDateDetectedConfidence: detectedDateInfo.confidence,
            ocDateDetectionMethod: detectedDateInfo.method,
            manualOcTargetOrderNumber: targetOrderNumberInput || detectedOrderNumber || null,
            detectedOrderNumber: detectedOrderNumber || null,
            rawPayloadForParser: null,
            excelText: scopedOrderText,
            excelAnalysis: syntheticAnalysis || excelAnalysis,
            warnings,
            timeline: [
                {
                    event: 'date_extracted',
                    fileType: extractedFile.fileType,
                    at: new Date().toISOString()
                }
            ]
        });

        return res.status(200).json({
            success: true,
            manualOcId,
            sourceClientCode: profile.sourceClientCode,
            sourceClientName: profile.sourceClientName,
            parserProfile: profile.parserProfile,
            fileType: extractedFile.fileType,
            targetOrderNumber: targetOrderNumberInput || detectedOrderNumber || null,
            detectedOrderNumber: detectedOrderNumber || null,
            ocDateDetected: detectedDateInfo.date || null,
            ocDateDetectedConfidence: detectedDateInfo.confidence,
            ocDateDetectionMethod: detectedDateInfo.method,
            excelPreview: extractedFile.excelPreview || null,
            excelAnalysis: syntheticAnalysis || excelAnalysis,
            warnings,
            canSubmitToInvoicer: false
        });
    } catch (error) {
        console.error('Error en readManualOcExtractDate:', error);
        return res.status(500).json({
            success: false,
            error: 'No se pudo extraer fecha manual OC',
            errorCode: 'extract_date_unhandled_exception',
            details: error?.message || String(error),
            canSubmitToInvoicer: false
        });
    }
}

async function readManualOcPreview(req, res) {
    try {
        const body = req.body || {};
        const sourceClientCode = String(body.sourceClientCode || '')
            .trim()
            .toUpperCase();
        const uploadedBy = String(body.uploadedBy || '').trim();
        const fileName = String(body.fileName || '').trim();
        const mimeType = String(body.mimeType || '').trim();
        const fileSize = Number(body.fileSize || 0);
        const fileBase64 = body.fileBase64;

        if (!uploadedBy) {
            return res.status(400).json({
                success: false,
                error: 'uploadedBy es requerido'
            });
        }

        if (!fileName || !fileBase64) {
            return res.status(400).json({
                success: false,
                error: 'fileName y fileBase64 son requeridos'
            });
        }

        const profile = getManualOcClientProfile(sourceClientCode);
        if (!profile) {
            return res.status(400).json({
                success: false,
                error: `sourceClientCode no soportado: ${sourceClientCode}`
            });
        }

        const fileBuffer = decodeFileBase64ToBuffer(fileBase64);
        const fileSha256 = createHash('sha256').update(fileBuffer).digest('hex');
        let extractedFile;
        try {
            extractedFile = await extractManualOcTextFromFile({
                fileName,
                mimeType,
                fileBuffer
            });
        } catch (fileError) {
            return res.status(400).json({
                success: false,
                error: fileError?.message || 'No se pudo leer el archivo'
            });
        }
        const excelText = extractedFile.text;
        const excelAnalysis = extractedFile?.excelPreview?.analysis || null;
        const detectedOrderNumber = (
            extractManualOcOrderNumber({ fileName, text: extractedFile.text })
            || normalizeManualOcOrderNumber(excelAnalysis?.purchaseOrderNumber)
        );
        const backendDuplicateRecord = detectedOrderNumber
            ? await findLatestManualOcByDetectedOrderNumber({
                sourceClientCode: profile.sourceClientCode,
                detectedOrderNumber,
                statuses: MANUAL_OC_PROCESSED_STATUSES
            })
            : null;
        if (backendDuplicateRecord) {
            return res.status(409).json({
                success: false,
                error: 'Orden de compra ya procesada en backend',
                duplicate: true,
                duplicateSource: 'backend',
                detectedOrderNumber,
                backendRecord: formatManualOcBackendDuplicate(backendDuplicateRecord)
            });
        }

        const detectedDateInfo = detectOcDateFromText(excelText);
        const manualOcId = randomUUID();
        const emailDateForPreview = detectedDateInfo.date
            ? `${detectedDateInfo.date}T12:00:00-03:00`
            : new Date().toISOString();

        const payload = buildManualReadEmailPayload({
            manualOcId,
            profile,
            fileName,
            excelText,
            emailDate: emailDateForPreview,
            uploadedBy,
            targetOrderNumber: detectedOrderNumber || null
        });

        await createManualOcRecord({
            manualOcId,
            status: 'preview_processing',
            sourceClientCode: profile.sourceClientCode,
            sourceClientName: profile.sourceClientName,
            parserProfile: profile.parserProfile,
            syntheticSender: profile.syntheticSender,
            uploadedBy,
            fileMeta: {
                fileName,
                mimeType,
                fileSize,
                fileSha256,
                fileType: extractedFile.fileType
            },
            ocDateDetected: detectedDateInfo.date || null,
            ocDateDetectedConfidence: detectedDateInfo.confidence,
            ocDateDetectionMethod: detectedDateInfo.method,
            detectedOrderNumber: detectedOrderNumber || null,
            rawPayloadForParser: payload,
            excelText,
            excelAnalysis,
            timeline: [
                {
                    event: 'preview_requested',
                    at: new Date().toISOString()
                }
            ]
        });

        const previewResponse = await runReadEmailBodyPayload(payload);
        const previewSuccess = previewResponse.status >= 200 && previewResponse.status < 300;

        const warnings = [];
        if (!detectedDateInfo.date) {
            warnings.push('No se detecto fecha OC automaticamente, usar fecha manual.');
        }
        if (extractedFile.fileType === 'excel' && excelAnalysis && !excelAnalysis.dates?.fechaEmision) {
            warnings.push('No se detecto Fecha emision en la estructura del Excel.');
        }
        if (extractedFile.fileType === 'excel' && excelAnalysis && (excelAnalysis.itemStats?.count || 0) === 0) {
            warnings.push('No se detectaron items en el bloque de detalle del Excel.');
        }
        if (!previewSuccess) {
            warnings.push('El parser no pudo construir merged valido en preview.');
        }

        await updateManualOcRecord(manualOcId, {
            status: previewSuccess ? 'preview_ready' : 'preview_failed',
            previewStatusCode: previewResponse.status,
            previewResponseBody: previewResponse.body,
            warnings
        });

        await appendManualOcTimeline(manualOcId, {
            event: previewSuccess ? 'preview_ready' : 'preview_failed',
            statusCode: previewResponse.status
        });

        return res.status(previewSuccess ? 200 : 422).json({
            success: previewSuccess,
            manualOcId,
            sourceClientCode: profile.sourceClientCode,
            sourceClientName: profile.sourceClientName,
            parserProfile: profile.parserProfile,
            fileType: extractedFile.fileType,
            detectedOrderNumber: detectedOrderNumber || null,
            ocDateDetected: detectedDateInfo.date || null,
            ocDateDetectedConfidence: detectedDateInfo.confidence,
            warnings,
            excelPreview: extractedFile.excelPreview || null,
            excelAnalysis,
            preview: {
                status: previewResponse.status,
                body: previewResponse.body
            }
        });
    } catch (error) {
        console.error('Error en readManualOcPreview:', error);
        return res.status(500).json({
            success: false,
            error: 'No se pudo procesar preview manual OC',
            details: error?.message || String(error)
        });
    }
}

function buildManualOcDispatchContextFromParserBody(parserResponse = {}) {
    const parserBody = parserResponse?.body && typeof parserResponse.body === 'object'
        ? parserResponse.body
        : {};
    const merged = parserBody?.merged && typeof parserBody.merged === 'object'
        ? parserBody.merged
        : {};

    return {
        emailData: merged?.EmailData || {},
        clientData: merged?.ClientData?.data || merged?.ClientData || {},
        deliveryReservation: parserBody?.deliveryReservation || null,
        parserMergedResult: {
            merged,
            deliveryReservation: parserBody?.deliveryReservation || null
        },
        parserStatus: Number.isFinite(Number(parserResponse?.status)) ? Number(parserResponse.status) : null,
        contextBuiltAt: new Date().toISOString()
    };
}

async function ensureManualOcDispatchContext({
    manualOcId,
    record,
    profile,
    uploadedBy,
    ocDateConfirmed
}) {
    const cachedContext = record?.dispatchContext || null;
    const cachedHasData = cachedContext
        && typeof cachedContext === 'object'
        && cachedContext.clientData
        && Object.keys(cachedContext.clientData || {}).length > 0;
    if (cachedHasData) {
        return {
            dispatchContext: cachedContext,
            fromCache: true
        };
    }

    const parserPayload = buildManualReadEmailPayload({
        manualOcId,
        profile,
        fileName: record?.fileMeta?.fileName || 'manual_oc.xlsx',
        excelText: record.excelText || '',
        emailDate: `${ocDateConfirmed}T09:00:00-03:00`,
        uploadedBy: uploadedBy || record.uploadedBy,
        targetOrderNumber: record?.manualOcTargetOrderNumber || record?.detectedOrderNumber || null
    });

    const parserResponse = await runReadEmailBodyPayload(parserPayload);
    const parserSuccess = parserResponse.status >= 200 && parserResponse.status < 300;
    if (!parserSuccess) {
        const parserError = new Error('No se pudo construir contexto de despacho');
        parserError.parser = {
            status: parserResponse.status,
            body: parserResponse.body
        };
        throw parserError;
    }

    const dispatchContext = buildManualOcDispatchContextFromParserBody(parserResponse);

    await updateManualOcRecord(manualOcId, {
        dispatchContext
    });
    await appendManualOcTimeline(manualOcId, {
        event: 'dispatch_context_built'
    });

    return {
        dispatchContext,
        fromCache: false
    };
}

async function readManualOcDispatchPreview(req, res) {
    try {
        const body = req.body || {};
        const manualOcId = String(body.manualOcId || '').trim();
        const ocDateConfirmed = String(body.ocDateConfirmed || '').trim();
        const arrivalDate = String(body.arrivalDate || '').trim();
        const arrivalMeridiem = normalizeManualOcArrivalMeridiem(body.arrivalMeridiem);
        const uploadedBy = String(body.uploadedBy || '').trim();

        if (!manualOcId) {
            return res.status(400).json({
                success: false,
                error: 'manualOcId es requerido',
                errorCode: 'manual_oc_id_required',
                canSubmitToInvoicer: false
            });
        }

        const record = await findManualOcRecord(manualOcId);
        if (!record) {
            return res.status(404).json({
                success: false,
                error: `No existe registro manual OC (${manualOcId})`,
                errorCode: 'manual_oc_not_found',
                canSubmitToInvoicer: false
            });
        }

        const profile = getManualOcClientProfile(record.sourceClientCode);
        if (!profile) {
            return res.status(400).json({
                success: false,
                error: `sourceClientCode no soportado: ${record.sourceClientCode}`,
                errorCode: 'source_client_code_not_supported',
                canSubmitToInvoicer: false
            });
        }

        const confirmedDate = parseManualDateCandidate(
            ocDateConfirmed
            || record.ocDateConfirmed
            || record.ocDateDetected
        );
        if (!confirmedDate) {
            return res.status(400).json({
                success: false,
                error: 'ocDateConfirmed invalida. Use formato YYYY-MM-DD o DD/MM/YYYY',
                errorCode: 'invalid_oc_date_confirmed',
                canSubmitToInvoicer: false
            });
        }

        let dispatchContext;
        let dispatchContextFromCache = false;
        try {
            const resolvedContext = await ensureManualOcDispatchContext({
                manualOcId,
                record,
                profile,
                uploadedBy,
                ocDateConfirmed: confirmedDate
            });
            dispatchContext = resolvedContext.dispatchContext;
            dispatchContextFromCache = resolvedContext.fromCache === true;
        } catch (contextError) {
            if (contextError?.parser) {
                const dispatchAnalysis = classifyManualOcParserFailure(contextError.parser);
                try {
                    await updateManualOcRecord(manualOcId, {
                        dispatchAnalysis,
                        dispatchAnalysisUpdatedAt: new Date().toISOString()
                    });
                    await appendManualOcTimeline(manualOcId, {
                        event: 'dispatch_preview_failed',
                        analysisCode: dispatchAnalysis.code,
                        analysisReason: dispatchAnalysis.reason
                    });
                } catch (trackingError) {
                    console.warn(
                        `No se pudo guardar dispatch_preview_failed para ${manualOcId}: ${trackingError?.message || trackingError}`
                    );
                }
                return res.status(422).json({
                    success: false,
                    errorCode: 'dispatch_preview_parser_failed',
                    error: 'No se pudo calcular preview de despacho',
                    parser: contextError.parser,
                    analysis: dispatchAnalysis,
                    canSubmitToInvoicer: false
                });
            }
            throw contextError;
        }

        const cutoffHourForDispatch = resolveManualOcDispatchCutoffHour({
            clientData: dispatchContext?.clientData || {},
            emailData: dispatchContext?.emailData || {}
        });
        const arrivalInfo = buildManualOcArrivalDateTime({
            arrivalDate: arrivalDate || confirmedDate,
            arrivalMeridiem,
            fallbackDate: confirmedDate,
            cutoffHourOverride: cutoffHourForDispatch
        });
        if (!arrivalInfo) {
            return res.status(400).json({
                success: false,
                error: 'arrivalDate invalida. Use formato YYYY-MM-DD o DD/MM/YYYY',
                errorCode: 'invalid_arrival_date',
                canSubmitToInvoicer: false
            });
        }

        const dispatchPreviewDate = resolveManualOcDeliveryDay({
            deliveryReservation: dispatchContext?.deliveryReservation || null,
            clientData: dispatchContext?.clientData || {},
            emailData: dispatchContext?.emailData || {},
            ocDateConfirmed: confirmedDate,
            arrivalDateTime: arrivalInfo.dateTimeIso
        }) || null;

        const clientData = dispatchContext?.clientData || {};
        const comuna = getObjectValueByKeys(clientData, [
            'Comuna Despacho',
            'Comuna despacho',
            'Comuna'
        ]);

        const dispatchAnalysis = buildManualOcSuccessAnalysis({
            code: 'dispatch_preview_ready',
            reason: 'Analisis de despacho correcto'
        });

        await updateManualOcRecord(manualOcId, {
            ocDateConfirmed: confirmedDate,
            arrivalDate: arrivalInfo.date,
            arrivalMeridiem: arrivalInfo.meridiem,
            dispatchPreviewDate,
            dispatchPreviewAt: new Date().toISOString(),
            dispatchAnalysis,
            dispatchAnalysisUpdatedAt: new Date().toISOString()
        });
        await appendManualOcTimeline(manualOcId, {
            event: 'dispatch_preview_calculated',
            ocDateConfirmed: confirmedDate,
            arrivalDate: arrivalInfo.date,
            arrivalMeridiem: arrivalInfo.meridiem,
            dispatchPreviewDate,
            analysisCode: dispatchAnalysis.code
        });

        return res.status(200).json({
            success: true,
            manualOcId,
            ocDateConfirmed: confirmedDate,
            arrivalDate: arrivalInfo.date,
            arrivalMeridiem: arrivalInfo.meridiem,
            arrivalDateTime: arrivalInfo.dateTimeIso,
            dispatchPreviewDate,
            comuna: comuna ? String(comuna).trim() : null,
            dispatchContextFromCache,
            analysis: dispatchAnalysis,
            canSubmitToInvoicer: true
        });
    } catch (error) {
        console.error('Error en readManualOcDispatchPreview:', error);
        return res.status(500).json({
            success: false,
            error: 'No se pudo calcular preview de despacho manual OC',
            errorCode: 'dispatch_preview_unhandled_exception',
            details: error?.message || String(error),
            canSubmitToInvoicer: false
        });
    }
}

async function readManualOcSubmit(req, res) {
    let trackedManualOcId = '';
    let submitLockToken = '';
    let submitLockSourceClientCode = '';
    let submitLockDetectedOrderNumber = '';
    try {
        const body = req.body || {};
        const manualOcId = String(body.manualOcId || '').trim();
        trackedManualOcId = manualOcId;
        const ocDateConfirmed = String(body.ocDateConfirmed || '').trim();
        const arrivalDate = String(body.arrivalDate || '').trim();
        const arrivalMeridiem = normalizeManualOcArrivalMeridiem(body.arrivalMeridiem);
        const uploadedBy = String(body.uploadedBy || '').trim();
        const developerMode = parseBooleanLoose(body.developerMode, MANUAL_OC_DEVELOPER_MODE_DEFAULT);
        const makeMode = String(body.makeMode || MANUAL_OC_MAKE_MODE_DEFAULT).trim() || MANUAL_OC_MAKE_MODE_DEFAULT;
        const testMode = parseBooleanLoose(body.testMode, MANUAL_OC_MAKE_TEST_MODE_DEFAULT);
        const preventBilling = parseBooleanLoose(body.preventBilling, MANUAL_OC_MAKE_PREVENT_BILLING_DEFAULT);

        if (!manualOcId) {
            return res.status(400).json({
                success: false,
                error: 'manualOcId es requerido',
                errorCode: 'manual_oc_id_required',
                canSubmitToInvoicer: false
            });
        }

        const record = await findManualOcRecord(manualOcId);
        if (!record) {
            return res.status(404).json({
                success: false,
                error: `No existe registro manual OC (${manualOcId})`,
                errorCode: 'manual_oc_not_found',
                canSubmitToInvoicer: false
            });
        }

        const recordStatus = String(record.status || '').trim();
        if (recordStatus === 'submit_processing' && record.makeResult) {
            const recoveredStatus = record.makeResult.delivered
                ? 'submitted_to_make'
                : (record.makeResult.skipped ? 'submit_skipped_make' : 'submit_failed_make');
            await updateManualOcRecord(manualOcId, {
                status: recoveredStatus
            });
            await appendManualOcTimeline(manualOcId, {
                event: 'submit_recovered_from_processing',
                recoveredStatus
            });

            return res.status(200).json({
                success: true,
                manualOcId,
                sourceClientCode: record.sourceClientCode || null,
                parserProfile: record.parserProfile || null,
                developerMode: record.makeResult?.developerMode === true,
                ocDateDetected: record.ocDateDetected || null,
                ocDateConfirmed: record.ocDateConfirmed || null,
                arrivalDate: record.arrivalDate || null,
                arrivalMeridiem: record.arrivalMeridiem || null,
                parser: {
                    status: record.submitParserStatusCode || null,
                    body: record.submitParserResponseBody || null
                },
                make: record.makeResult,
                makeConfig: {
                    mode: record.submitMakeMode || null,
                    testMode: typeof record.submitTestMode === 'boolean' ? record.submitTestMode : null,
                    preventBilling: typeof record.submitPreventBilling === 'boolean' ? record.submitPreventBilling : null
                },
                analysis: record.dispatchAnalysis || null,
                canSubmitToInvoicer: record.makeResult?.delivered === true || record.makeResult?.skipped === true
            });
        }

        const finalStatuses = new Set(['submitted_to_make', 'submit_skipped_make']);
        if (finalStatuses.has(recordStatus) && record.makeResult) {
            return res.status(200).json({
                success: true,
                manualOcId,
                sourceClientCode: record.sourceClientCode || null,
                parserProfile: record.parserProfile || null,
                developerMode: record.makeResult?.developerMode === true,
                ocDateDetected: record.ocDateDetected || null,
                ocDateConfirmed: record.ocDateConfirmed || null,
                arrivalDate: record.arrivalDate || null,
                arrivalMeridiem: record.arrivalMeridiem || null,
                parser: {
                    status: record.submitParserStatusCode || null,
                    body: record.submitParserResponseBody || null
                },
                make: record.makeResult,
                makeConfig: {
                    mode: record.submitMakeMode || null,
                    testMode: typeof record.submitTestMode === 'boolean' ? record.submitTestMode : null,
                    preventBilling: typeof record.submitPreventBilling === 'boolean' ? record.submitPreventBilling : null
                },
                analysis: record.dispatchAnalysis || null,
                canSubmitToInvoicer: record.makeResult?.delivered === true || record.makeResult?.skipped === true
            });
        }

        if (recordStatus === 'submit_processing') {
            const updatedAtMs = new Date(record.updatedAt || 0).getTime();
            const staleMs = 2 * 60 * 1000;
            if (Number.isFinite(updatedAtMs) && (Date.now() - updatedAtMs) < staleMs) {
                return res.status(409).json({
                    success: false,
                    error: 'La OC manual ya se encuentra en procesamiento',
                    errorCode: 'manual_oc_already_processing',
                    manualOcId,
                    status: recordStatus,
                    canSubmitToInvoicer: false
                });
            }
        }

        const profile = getManualOcClientProfile(record.sourceClientCode);
        if (!profile) {
            return res.status(400).json({
                success: false,
                error: `sourceClientCode no soportado: ${record.sourceClientCode}`,
                errorCode: 'source_client_code_not_supported',
                canSubmitToInvoicer: false
            });
        }

        const detectedOrderNumber = normalizeManualOcOrderNumber(record.detectedOrderNumber);
        if (detectedOrderNumber) {
            const duplicateProcessedRecord = await findLatestManualOcByDetectedOrderNumber({
                sourceClientCode: profile.sourceClientCode,
                detectedOrderNumber,
                statuses: MANUAL_OC_PROCESSED_STATUSES
            });
            const duplicateManualOcId = String(duplicateProcessedRecord?.manualOcId || '').trim();

            if (duplicateProcessedRecord && duplicateManualOcId && duplicateManualOcId !== manualOcId) {
                await updateManualOcRecord(manualOcId, {
                    status: 'submit_blocked_duplicate_backend',
                    submitError: `OC ${detectedOrderNumber} ya tiene procesamiento activo o finalizado (${duplicateManualOcId})`
                });
                await appendManualOcTimeline(manualOcId, {
                    event: 'submit_blocked_duplicate_backend',
                    detectedOrderNumber,
                    duplicateManualOcId
                });

                return res.status(409).json({
                    success: false,
                    error: 'Orden de compra ya procesada o en procesamiento',
                    errorCode: 'duplicate_backend_submit_guard',
                    duplicate: true,
                    duplicateSource: 'backend_submit_guard',
                    detectedOrderNumber,
                    backendRecord: formatManualOcBackendDuplicate(duplicateProcessedRecord),
                    canSubmitToInvoicer: false
                });
            }
        }

        const confirmedDate = parseManualDateCandidate(ocDateConfirmed || record.ocDateDetected);
        if (!confirmedDate) {
            return res.status(400).json({
                success: false,
                error: 'ocDateConfirmed invalida. Use formato YYYY-MM-DD o DD/MM/YYYY',
                errorCode: 'invalid_oc_date_confirmed',
                canSubmitToInvoicer: false
            });
        }

        if (detectedOrderNumber) {
            submitLockToken = `${manualOcId}:${randomUUID()}`;
            submitLockSourceClientCode = profile.sourceClientCode;
            submitLockDetectedOrderNumber = detectedOrderNumber;
            const lockResult = await acquireManualOcSubmitLock({
                sourceClientCode: submitLockSourceClientCode,
                detectedOrderNumber: submitLockDetectedOrderNumber,
                ownerToken: submitLockToken
            });

            if (!lockResult?.ok) {
                await updateManualOcRecord(manualOcId, {
                    status: 'submit_blocked_by_lock',
                    submitError: `OC ${detectedOrderNumber} bloqueada por otra solicitud activa`
                });
                await appendManualOcTimeline(manualOcId, {
                    event: 'submit_blocked_by_lock',
                    detectedOrderNumber,
                    lockExpiresAt: lockResult?.expiresAt || null
                });

                return res.status(409).json({
                    success: false,
                    error: 'Orden de compra en procesamiento por otra solicitud',
                    errorCode: 'submit_lock_conflict',
                    duplicate: true,
                    duplicateSource: 'submit_lock',
                    detectedOrderNumber,
                    lock: {
                        expiresAt: lockResult?.expiresAt || null
                    },
                    canSubmitToInvoicer: false
                });
            }
        }

        let dispatchContext = record?.dispatchContext || null;
        if (!dispatchContext) {
            try {
                const resolvedContext = await ensureManualOcDispatchContext({
                    manualOcId,
                    record,
                    profile,
                    uploadedBy,
                    ocDateConfirmed: confirmedDate
                });
                dispatchContext = resolvedContext.dispatchContext;
            } catch (contextError) {
                console.warn('No se pudo cargar dispatchContext para cutoff en submit manual OC:', contextError?.message || contextError);
            }
        }

        const cutoffHourForDispatch = resolveManualOcDispatchCutoffHour({
            clientData: dispatchContext?.clientData || {},
            emailData: dispatchContext?.emailData || {}
        });
        const arrivalInfo = buildManualOcArrivalDateTime({
            arrivalDate: arrivalDate || confirmedDate,
            arrivalMeridiem,
            fallbackDate: confirmedDate,
            cutoffHourOverride: cutoffHourForDispatch
        });
        if (!arrivalInfo) {
            return res.status(400).json({
                success: false,
                error: 'arrivalDate invalida. Use formato YYYY-MM-DD o DD/MM/YYYY',
                errorCode: 'invalid_arrival_date',
                canSubmitToInvoicer: false
            });
        }

        await updateManualOcRecord(manualOcId, {
            status: 'submit_processing',
            ocDateConfirmed: confirmedDate,
            arrivalDate: arrivalInfo.date,
            arrivalMeridiem: arrivalInfo.meridiem,
            submitRequestedBy: uploadedBy || record.uploadedBy,
            submitDeveloperMode: developerMode,
            submitMakeMode: makeMode,
            submitTestMode: testMode,
            submitPreventBilling: preventBilling
        });
        await appendManualOcTimeline(manualOcId, {
            event: 'submit_requested',
            ocDateConfirmed: confirmedDate,
            arrivalDate: arrivalInfo.date,
            arrivalMeridiem: arrivalInfo.meridiem,
            developerMode,
            makeMode,
            testMode,
            preventBilling
        });

        const cachedParserMergedResult = dispatchContext?.parserMergedResult
            && typeof dispatchContext.parserMergedResult === 'object'
            ? dispatchContext.parserMergedResult
            : null;
        let parserResponse = null;
        let parserException = null;
        const parserAttempts = 2;
        if (cachedParserMergedResult?.merged) {
            parserResponse = {
                status: Number.isFinite(Number(dispatchContext?.parserStatus))
                    ? Number(dispatchContext.parserStatus)
                    : 200,
                body: cachedParserMergedResult
            };
        } else {
            const emailDateToUse = arrivalInfo.dateTimeIso;
            const payload = buildManualReadEmailPayload({
                manualOcId,
                profile,
                fileName: record?.fileMeta?.fileName || 'manual_oc.xlsx',
                excelText: record.excelText || '',
                emailDate: emailDateToUse,
                uploadedBy: uploadedBy || record.uploadedBy,
                targetOrderNumber: record?.manualOcTargetOrderNumber || record?.detectedOrderNumber || null
            });
            for (let attempt = 1; attempt <= parserAttempts; attempt += 1) {
                try {
                    parserResponse = await runReadEmailBodyPayload(payload);
                    parserException = null;
                    break;
                } catch (currentParserError) {
                    parserException = currentParserError;
                    if (attempt < parserAttempts) {
                        await new Promise((resolve) => setTimeout(resolve, 250));
                    }
                }
            }
        }

        if (parserException) {
            const submitAnalysis = {
                analyzed: true,
                ok: false,
                canSubmitToInvoicer: false,
                code: 'parser_exception',
                reason: parserException?.message || String(parserException)
            };
            await updateManualOcRecord(manualOcId, {
                status: 'submit_failed_parser_exception',
                submitError: parserException?.message || String(parserException),
                dispatchAnalysis: submitAnalysis,
                dispatchAnalysisUpdatedAt: new Date().toISOString()
            });
            await appendManualOcTimeline(manualOcId, {
                event: 'submit_failed_parser_exception',
                attempts: parserAttempts,
                errorMessage: parserException?.message || String(parserException),
                analysisCode: submitAnalysis.code
            });

            return res.status(502).json({
                errorCode: 'submit_parser_exception',
                success: false,
                error: 'El parser lanzo una excepcion durante el submit manual OC',
                details: parserException?.message || String(parserException),
                parser: {
                    attempts: parserAttempts
                },
                analysis: submitAnalysis,
                canSubmitToInvoicer: false
            });
        }

        const parserSuccess = parserResponse.status >= 200 && parserResponse.status < 300;

        if (!parserSuccess) {
            const submitAnalysis = classifyManualOcParserFailure({
                status: parserResponse.status,
                body: parserResponse.body
            });
            await updateManualOcRecord(manualOcId, {
                status: 'submit_failed_parser',
                submitParserStatusCode: parserResponse.status,
                submitParserResponseBody: parserResponse.body,
                dispatchAnalysis: submitAnalysis,
                dispatchAnalysisUpdatedAt: new Date().toISOString()
            });
            await appendManualOcTimeline(manualOcId, {
                event: 'submit_failed_parser',
                statusCode: parserResponse.status,
                analysisCode: submitAnalysis.code,
                analysisReason: submitAnalysis.reason
            });

            return res.status(422).json({
                errorCode: 'submit_parser_failed',
                success: false,
                error: 'El parser no pudo construir merged valido para submit',
                parser: {
                    status: parserResponse.status,
                    body: parserResponse.body
                },
                analysis: submitAnalysis,
                canSubmitToInvoicer: false
            });
        }

        if (!cachedParserMergedResult?.merged) {
            try {
                const refreshedDispatchContext = buildManualOcDispatchContextFromParserBody(parserResponse);
                dispatchContext = refreshedDispatchContext;
                await updateManualOcRecord(manualOcId, {
                    dispatchContext: refreshedDispatchContext
                });
                await appendManualOcTimeline(manualOcId, {
                    event: 'dispatch_context_built_during_submit'
                });
            } catch (contextPersistError) {
                console.warn(
                    `No se pudo persistir dispatchContext durante submit para ${manualOcId}: ${contextPersistError?.message || contextPersistError}`
                );
            }
        }

        const makeResult = await sendManualMergedToMake({
            manualOcId,
            profile,
            mergedResult: parserResponse.body,
            ocDateDetected: record.ocDateDetected || null,
            ocDateConfirmed: confirmedDate,
            arrivalDate: arrivalInfo.date,
            arrivalMeridiem: arrivalInfo.meridiem,
            arrivalDateTime: arrivalInfo.dateTimeIso,
            uploadedBy: uploadedBy || record.uploadedBy,
            fileMeta: record.fileMeta || null,
            developerMode,
            makeOptions: {
                mode: makeMode,
                testMode,
                preventBilling
            },
            submitRequest: {
                uiSubmitBody: body,
                manualOcId,
                uploadedBy: uploadedBy || record.uploadedBy,
                sourceClientCode: profile.sourceClientCode,
                ocDateConfirmed: confirmedDate,
                arrivalDate: arrivalInfo.date,
                arrivalMeridiem: arrivalInfo.meridiem,
                arrivalDateTime: arrivalInfo.dateTimeIso,
                developerMode,
                makeMode,
                testMode,
                preventBilling
            }
        });

        const finalStatus = makeResult.delivered
            ? 'submitted_to_make'
            : (makeResult.skipped ? 'submit_skipped_make' : 'submit_failed_make');
        const submitAnalysis = makeResult.delivered || makeResult.skipped
            ? buildManualOcSuccessAnalysis({
                code: finalStatus,
                reason: makeResult.delivered
                    ? 'Fila enviada correctamente al facturador'
                    : 'Fila omitida en modo desarrollador'
            })
            : {
                analyzed: true,
                ok: false,
                canSubmitToInvoicer: false,
                code: finalStatus,
                reason: String(makeResult?.error || makeResult?.message || 'No se pudo enviar la fila al facturador')
            };

        await updateManualOcRecord(manualOcId, {
            status: finalStatus,
            submitParserStatusCode: parserResponse.status,
            submitParserResponseBody: parserResponse.body,
            makeResult,
            dispatchAnalysis: submitAnalysis,
            dispatchAnalysisUpdatedAt: new Date().toISOString()
        });
        await appendManualOcTimeline(manualOcId, {
            event: finalStatus,
            makeStatus: makeResult.status || null,
            makeDelivered: makeResult.delivered === true,
            developerMode,
            payloadDumpPath: makeResult.payloadDumpPath || null,
            analysisCode: submitAnalysis.code
        });

        const statusCode = makeResult.delivered || makeResult.skipped ? 200 : 502;
        return res.status(statusCode).json({
            success: makeResult.delivered || makeResult.skipped,
            manualOcId,
            sourceClientCode: profile.sourceClientCode,
            parserProfile: profile.parserProfile,
            developerMode,
            makeConfig: {
                mode: makeMode,
                testMode,
                preventBilling
            },
            ocDateDetected: record.ocDateDetected || null,
            ocDateConfirmed: confirmedDate,
            arrivalDate: arrivalInfo.date,
            arrivalMeridiem: arrivalInfo.meridiem,
            arrivalDateTime: arrivalInfo.dateTimeIso,
            parser: {
                status: parserResponse.status,
                body: parserResponse.body
            },
            make: makeResult,
            analysis: submitAnalysis,
            canSubmitToInvoicer: makeResult.delivered || makeResult.skipped
        });
    } catch (error) {
        console.error('Error en readManualOcSubmit:', error);
        if (trackedManualOcId) {
            try {
                await updateManualOcRecord(trackedManualOcId, {
                    status: 'submit_failed_exception',
                    submitError: error?.message || String(error)
                });
                await appendManualOcTimeline(trackedManualOcId, {
                    event: 'submit_failed_exception',
                    errorMessage: error?.message || String(error)
                });
            } catch (trackingError) {
                console.warn(
                    `No se pudo guardar submit_failed_exception para ${trackedManualOcId}: ${trackingError?.message || trackingError}`
                );
            }
        }
        return res.status(500).json({
            success: false,
            error: 'No se pudo completar submit manual OC',
            errorCode: 'submit_unhandled_exception',
            details: error?.message || String(error),
            canSubmitToInvoicer: false
        });
    } finally {
        if (submitLockToken && submitLockSourceClientCode && submitLockDetectedOrderNumber) {
            try {
                await releaseManualOcSubmitLock({
                    sourceClientCode: submitLockSourceClientCode,
                    detectedOrderNumber: submitLockDetectedOrderNumber,
                    ownerToken: submitLockToken
                });
            } catch (lockReleaseError) {
                console.warn(
                    `No se pudo liberar submit lock ${submitLockSourceClientCode}/${submitLockDetectedOrderNumber}: ${lockReleaseError?.message || lockReleaseError}`
                );
            }
        }
    }
}

async function readEmailBodyFromGmail(req, res) {
    const requestBody = req.body || {};
    const messageId = typeof requestBody === 'string' ? requestBody.trim() : requestBody.messageId;
    if (!messageId) {
        return res.status(400).json({ success: false, error: 'messageId es requerido' });
    }

    try {
        const { gmail, userId } = await buildGmailClient();
        const message = await gmail.users.messages.get({
            userId,
            id: messageId,
            format: 'full'
        });

        const headers = headersToMap(message.data?.payload?.headers || []);
        const sender = extractEmailAddress(headers.From || '').toLowerCase();
        const isRappiSender = String(sender || '').toLowerCase().endsWith('@rappi.com') || String(sender || '').toLowerCase().endsWith('@rappi.cl');
        const allowedSenders = new Set([
            PEDIDOS_YA_SENDER,
            KEY_LOGISTICS_SENDER
        ]);

        if (!allowedSenders.has(sender) && !isRappiSender) {
            return res.status(403).json({
                success: false,
                error: 'Emisor no permitido',
                sender,
                allowedSenders: Array.from(allowedSenders),
                isRappiSender
            });
        }

        const emailBody = extractEmailText(message.data?.payload);
        const emailSubject = headers.Subject || '';
        const emailDate = message.data?.internalDate
            ? new Date(Number(message.data.internalDate)).toISOString()
            : (headers.Date || '');

        if (sender === KEY_LOGISTICS_SENDER) {
            const pdfAttachments = findPdfAttachments(message.data?.payload);
            if (pdfAttachments.length === 0) {
                return res.status(400).json({
                    success: false,
                    error: 'No se encontraron adjuntos PDF',
                    messageId
                });
            }

            const results = [];
            const seenOrders = new Set();
            for (const attachment of pdfAttachments) {
                try {
                    const attachmentResponse = await gmail.users.messages.attachments.get({
                        userId,
                        messageId,
                        id: attachment.attachmentId
                    });

                    const buffer = decodeBase64Url(attachmentResponse.data?.data);
                    const pdfText = await pdfBufferToText(buffer);
                    const keyLogisticsData = parseKeyLogisticsOrderText(pdfText);
                    const savedFiles = await saveTempFiles(messageId, attachment.filename, buffer, pdfText);

                    const orderKey = keyLogisticsData?.clientId && keyLogisticsData?.ocNumber
                        ? `${keyLogisticsData.clientId}:${keyLogisticsData.ocNumber}`
                        : null;

                    if (orderKey && seenOrders.has(orderKey)) {
                        results.push({
                            filename: attachment.filename,
                            mimeType: attachment.mimeType,
                            savedFiles,
                            keyLogistics: keyLogisticsData,
                            duplicateCheck: null,
                            status: 409,
                            response: {
                                success: false,
                                error: 'Orden de compra duplicada en el mismo correo',
                                duplicate: true,
                                duplicateInMessage: true,
                                ocNumber: keyLogisticsData.ocNumber,
                                clientId: keyLogisticsData.clientId
                            }
                        });
                        continue;
                    }

                    if (orderKey) {
                        seenOrders.add(orderKey);
                        try {
                            const existingOrder = await findProcessedKeyLogisticsOrder({
                                clientId: keyLogisticsData.clientId,
                                ocNumber: keyLogisticsData.ocNumber
                            });
                            if (existingOrder) {
                                results.push({
                                    filename: attachment.filename,
                                    mimeType: attachment.mimeType,
                                    savedFiles,
                                    keyLogistics: keyLogisticsData,
                                    duplicateCheck: null,
                                    status: 409,
                                    response: {
                                        success: false,
                                        error: 'Orden de compra duplicada',
                                        duplicate: true,
                                        ocNumber: keyLogisticsData.ocNumber,
                                        clientId: keyLogisticsData.clientId
                                    }
                                });
                                continue;
                            }
                        } catch (error) {
                            // Continue processing; the order won't be marked if validation fails.
                        }
                    }

                    const payload = {
                        emailBody,
                        emailSubject,
                        emailAttached: pdfText,
                        emailDate,
                        source: 'gmail',
                        sender,
                        attachmentFilename: attachment.filename,
                        keyLogistics: keyLogisticsData
                    };

                    const response = await runReadEmailBodyPayload(payload);

                    if (
                        orderKey &&
                        response.status === 200 &&
                        response.body?.success !== false
                    ) {
                        try {
                            await insertProcessedKeyLogisticsOrder({
                                clientId: keyLogisticsData.clientId,
                                ocNumber: keyLogisticsData.ocNumber,
                                sender,
                                messageId,
                                emailDate,
                                attachmentFilename: attachment.filename,
                                quantities: keyLogisticsData.quantities
                            });
                        } catch (error) {
                            // Continue without blocking the response; duplicates are handled by unique index.
                        }
                    }

                    results.push({
                        filename: attachment.filename,
                        mimeType: attachment.mimeType,
                        savedFiles,
                        keyLogistics: keyLogisticsData,
                        duplicateCheck: null,
                        status: response.status,
                        response: response.body
                    });
                } catch (error) {
                    results.push({
                        filename: attachment.filename,
                        mimeType: attachment.mimeType,
                        status: 500,
                        response: { success: false, error: error.message }
                    });
                }
            }

            const success = results.every((result) => result.status >= 200 && result.status < 300);
            return res.status(200).json({
                success,
                messageId,
                sender,
                subject: emailSubject,
                emailDate,
                attachments: pdfAttachments.length,
                results
            });
        }

        if (isRappiSender) {
            const pdfAttachments = findPdfAttachments(message.data?.payload);
            if (pdfAttachments.length === 0) {
                return res.status(400).json({
                    success: false,
                    error: 'No se encontraron adjuntos PDF',
                    messageId
                });
            }

            const results = [];
            const seenOcNumbers = new Set();

            for (const attachment of pdfAttachments) {
                try {
                    const attachmentResponse = await gmail.users.messages.attachments.get({
                        userId,
                        messageId,
                        id: attachment.attachmentId
                    });

                    const buffer = decodeBase64Url(attachmentResponse.data?.data);
                    const pdfText = await pdfBufferToText(buffer);
                    const rappiTurboData = parseRappiTurboOrderText(pdfText);
                    const savedFiles = await saveTempFiles(messageId, attachment.filename, buffer, pdfText);
                    const ocNumber = rappiTurboData?.ocNumber || null;

                    if (ocNumber && seenOcNumbers.has(ocNumber)) {
                        results.push({
                            filename: attachment.filename,
                            mimeType: attachment.mimeType,
                            savedFiles,
                            rappiTurbo: rappiTurboData,
                            status: 409,
                            response: {
                                success: false,
                                error: 'Orden de compra duplicada en el mismo correo',
                                duplicate: true,
                                duplicateInMessage: true,
                                ocNumber
                            }
                        });
                        continue;
                    }

                    if (ocNumber) {
                        seenOcNumbers.add(ocNumber);
                        try {
                            const existingOrder = await findProcessedSenderOrder({
                                sender,
                                ocNumber,
                                source: 'rappi_turbo'
                            });
                            if (existingOrder) {
                                results.push({
                                    filename: attachment.filename,
                                    mimeType: attachment.mimeType,
                                    savedFiles,
                                    rappiTurbo: rappiTurboData,
                                    status: 409,
                                    response: {
                                        success: false,
                                        error: 'Orden de compra duplicada',
                                        duplicate: true,
                                        ocNumber
                                    }
                                });
                                continue;
                            }
                        } catch (error) {
                            // Continue processing even if duplicate check cannot be validated.
                        }
                    }

                    const payload = {
                        emailBody,
                        emailSubject,
                        emailAttached: pdfText,
                        emailDate,
                        source: 'gmail',
                        sender,
                        attachmentFilename: attachment.filename,
                        rappiTurbo: rappiTurboData
                    };

                    const response = await runReadEmailBodyPayload(payload);

                    if (
                        ocNumber &&
                        response.status === 200 &&
                        response.body?.success !== false
                    ) {
                        try {
                            await insertProcessedSenderOrder({
                                sender,
                                ocNumber,
                                messageId,
                                emailDate,
                                attachmentFilename: attachment.filename,
                                quantities: rappiTurboData?.quantities,
                                metadata: {
                                    rut: rappiTurboData?.rut || null,
                                    storeName: rappiTurboData?.storeName || null,
                                    dispatchAddress: rappiTurboData?.dispatchAddress || null,
                                    totals: rappiTurboData?.totals || null
                                },
                                source: 'rappi_turbo'
                            });
                        } catch (error) {
                            // Continue without blocking the response; duplicates are handled by unique index.
                        }
                    }

                    results.push({
                        filename: attachment.filename,
                        mimeType: attachment.mimeType,
                        savedFiles,
                        rappiTurbo: rappiTurboData,
                        status: response.status,
                        response: response.body
                    });
                } catch (error) {
                    results.push({
                        filename: attachment.filename,
                        mimeType: attachment.mimeType,
                        status: 500,
                        response: { success: false, error: error.message }
                    });
                }
            }

            const success = results.every((result) => result.status >= 200 && result.status < 300);
            const allDuplicates = results.length > 0 && results.every((result) => result.status === 409);
            return res.status(allDuplicates ? 409 : 200).json({
                success,
                messageId,
                sender,
                subject: emailSubject,
                emailDate,
                attachments: pdfAttachments.length,
                results
            });
        }

        const excelAttachments = findExcelAttachments(message.data?.payload);
        if (excelAttachments.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'No se encontraron adjuntos Excel',
                messageId
            });
        }

        const results = [];
        for (const attachment of excelAttachments) {
            try {
                const attachmentResponse = await gmail.users.messages.attachments.get({
                    userId,
                    messageId,
                    id: attachment.attachmentId
                });

                const buffer = decodeBase64Url(attachmentResponse.data?.data);
                const excelText = excelBufferToText(buffer);
                const savedFiles = await saveTempFiles(messageId, attachment.filename, buffer, excelText);
                const payload = {
                    emailBody,
                    emailSubject,
                    emailAttached: excelText,
                    emailDate,
                    source: 'gmail',
                    sender,
                    attachmentFilename: attachment.filename
                };

                const response = await runReadEmailBodyPayload(payload);
                results.push({
                    filename: attachment.filename,
                    mimeType: attachment.mimeType,
                    savedFiles,
                    duplicateCheck: null,
                    status: response.status,
                    response: response.body
                });
            } catch (error) {
                results.push({
                    filename: attachment.filename,
                    mimeType: attachment.mimeType,
                    status: 500,
                    response: { success: false, error: error.message }
                });
            }
        }

        const success = results.every((result) => result.status >= 200 && result.status < 300);
        return res.status(200).json({
            success,
            messageId,
            sender,
            subject: emailSubject,
            emailDate,
            attachments: excelAttachments.length,
            results
        });
    } catch (error) {
        return res.status(500).json({
            success: false,
            error: 'No se pudo procesar el correo',
            details: error.message,
            messageId
        });
    }
}

function normalizeRut(rut) {
    // Remove all non-numeric characters except 'k' or 'K' (used in Chilean RUTs)
    rut = rut.replace(/[^0-9kK]/g, '');

    // Convert to uppercase for consistency
    rut = rut.toUpperCase();

    // Separate the RUT into the body and the verifier digit
    const body = rut.slice(0, -1);
    const verifier = rut.slice(-1);

    // Format the RUT with thousands separator and append the verifier digit
    const formattedRut = body.replace(/\B(?=(\d{3})+(?!\d))/g, '.') + '-' + verifier;

    return formattedRut;
}

const ADDRESS_TOKEN_CANONICAL_MAP = Object.freeze({
    avenida: 'av',
    avda: 'av',
    avd: 'av',
    calle: 'cl',
    psje: 'pasaje',
    pje: 'pasaje',
    numero: 'nro',
    num: 'nro',
    no: 'nro',
    nro: 'nro',
    depto: 'dpto',
    departamento: 'dpto',
    oficina: 'of'
});
const ADDRESS_GEO_NOISE_TOKENS = new Set([
    'region',
    'metropolitana',
    'rm',
    'chile',
    'provincia',
    'comuna'
]);
const ADDRESS_GENERIC_TOKENS = new Set([
    'av',
    'cl',
    'pasaje',
    'camino',
    'ruta',
    'km',
    'nro',
    'local',
    'loc',
    'of',
    'piso',
    'torre',
    'bodega',
    'de',
    'del',
    'la',
    'las',
    'los',
    'el',
    'y'
]);

function normalizeAddressText(value) {
    return String(value || '')
        .toLowerCase()
        .normalize('NFD')
        .replace(/\p{Diacritic}/gu, '')
        .replace(/[^a-z0-9]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function normalizeAddressToken(token) {
    const normalized = normalizeAddressText(token);
    if (!normalized) {
        return '';
    }
    return ADDRESS_TOKEN_CANONICAL_MAP[normalized] || normalized;
}

function buildAddressTokens(value, options = {}) {
    const dropGeoNoise = options?.dropGeoNoise !== false;
    const normalized = normalizeAddressText(value);
    if (!normalized) {
        return [];
    }

    return normalized
        .split(' ')
        .map((token) => normalizeAddressToken(token))
        .filter(Boolean)
        .filter((token) => {
            if (dropGeoNoise && ADDRESS_GEO_NOISE_TOKENS.has(token)) {
                return false;
            }
            if (token.length === 1 && !/^\d[a-z]?$/.test(token)) {
                return false;
            }
            return true;
        });
}

function extractAddressNumberTokens(valueOrTokens) {
    const tokens = Array.isArray(valueOrTokens)
        ? valueOrTokens
        : buildAddressTokens(valueOrTokens);
    return tokens.filter((token) => /^\d+[a-z]?$/.test(token));
}

function extractAddressKeywordTokens(tokens = []) {
    return tokens.filter((token) => {
        if (!token) {
            return false;
        }
        if (/^\d+[a-z]?$/.test(token)) {
            return false;
        }
        return !ADDRESS_GENERIC_TOKENS.has(token);
    });
}

function resolveClientDispatchAddress(row) {
    if (!row || typeof row !== 'object') {
        return '';
    }
    return String(
        getObjectValueByKeys(row, [
            'Direccion Despacho',
            'Direccion Despacho',
            'Direccion Despacho',
            'direccion despacho'
        ]) || ''
    ).trim();
}

function calculateAddressMatchScore(baseAddress, candidateAddress) {
    const baseTokens = buildAddressTokens(baseAddress);
    const candidateTokens = buildAddressTokens(candidateAddress);
    if (baseTokens.length === 0 || candidateTokens.length === 0) {
        return 0;
    }

    const baseCanonical = baseTokens.join(' ');
    const candidateCanonical = candidateTokens.join(' ');
    if (baseCanonical === candidateCanonical) {
        return 1;
    }

    const baseTokenSet = new Set(baseTokens);
    const candidateTokenSet = new Set(candidateTokens);
    if (baseTokenSet.size === 0 || candidateTokenSet.size === 0) {
        return 0;
    }

    const commonTokens = [];
    for (const token of candidateTokenSet) {
        if (baseTokenSet.has(token)) {
            commonTokens.push(token);
        }
    }
    if (commonTokens.length === 0) {
        return 0;
    }

    const baseNumbers = extractAddressNumberTokens(baseTokens);
    const candidateNumbers = extractAddressNumberTokens(candidateTokens);
    const hasBaseNumbers = baseNumbers.length > 0;
    const hasCandidateNumbers = candidateNumbers.length > 0;
    const baseNumbersSet = new Set(baseNumbers);
    const hasNumberOverlap = hasBaseNumbers && hasCandidateNumbers
        ? candidateNumbers.some((token) => baseNumbersSet.has(token))
        : true;
    if ((hasBaseNumbers && hasCandidateNumbers) && !hasNumberOverlap) {
        return 0.05;
    }

    const baseKeywords = extractAddressKeywordTokens(baseTokens);
    const candidateKeywords = extractAddressKeywordTokens(candidateTokens);
    const baseKeywordSet = new Set(baseKeywords);
    const keywordOverlapCount = candidateKeywords.filter((token) => baseKeywordSet.has(token)).length;
    if (baseKeywords.length > 0 && candidateKeywords.length > 0 && keywordOverlapCount === 0) {
        return hasNumberOverlap ? 0.25 : 0.05;
    }

    const unionTokenCount = new Set([...baseTokenSet, ...candidateTokenSet]).size;
    const precision = commonTokens.length / candidateTokenSet.size;
    const recall = commonTokens.length / baseTokenSet.size;
    const jaccard = unionTokenCount > 0 ? (commonTokens.length / unionTokenCount) : 0;
    const f1 = (precision + recall) > 0
        ? (2 * precision * recall) / (precision + recall)
        : 0;
    let score = Math.max(jaccard, f1, (precision * 0.65) + (recall * 0.35));

    if (baseCanonical.includes(candidateCanonical) || candidateCanonical.includes(baseCanonical)) {
        score = Math.max(score, 0.9);
    }

    if (hasNumberOverlap && keywordOverlapCount >= 2) {
        score = Math.max(score, 0.9);
    } else if (hasNumberOverlap && keywordOverlapCount >= 1) {
        score = Math.max(score, 0.82);
    }

    if (hasBaseNumbers !== hasCandidateNumbers) {
        score = Math.min(score, 0.74);
    }

    if (keywordOverlapCount === 0) {
        score = Math.min(score, 0.49);
    }

    return Math.max(0, Math.min(1, score));
}

async function resolveAddressMatchWithLlmFallback({ rankedMatches = [], requestedAddress = '' } = {}) {
    const fallbackCandidates = [];
    const indexedMatches = [];
    const seenAddresses = new Set();
    for (const rankedMatch of rankedMatches) {
        const rowAddress = String(rankedMatch?.rowAddress || '').trim();
        if (!rowAddress) {
            continue;
        }
        const normalizedRowAddress = normalizeAddressText(rowAddress);
        if (!normalizedRowAddress || seenAddresses.has(normalizedRowAddress)) {
            continue;
        }
        seenAddresses.add(normalizedRowAddress);
        indexedMatches.push(rankedMatch);
        fallbackCandidates.push({
            index: indexedMatches.length - 1,
            direccion: rowAddress
        });
        if (fallbackCandidates.length >= ADDRESS_MATCH_LLM_FALLBACK_TOPN) {
            break;
        }
    }

    if (fallbackCandidates.length === 0) {
        return null;
    }

    const llmMatches = await integrateWithChatGPT(fallbackCandidates, requestedAddress);
    const bestLlmMatch = llmMatches
        .filter((item) => item.match === true)
        .sort((a, b) => Number(b?.confidence || 0) - Number(a?.confidence || 0))[0];
    if (!bestLlmMatch) {
        return null;
    }

    const llmConfidence = Number(bestLlmMatch.confidence || 0);
    if (llmConfidence < ADDRESS_MATCH_MIN_CONFIDENCE) {
        return null;
    }

    const selectedRankedMatch = indexedMatches[Number(bestLlmMatch.index)];
    if (!selectedRankedMatch) {
        return null;
    }

    return {
        match: selectedRankedMatch,
        llm: bestLlmMatch
    };
}

async function findClientByAddressInCsv(addressCandidates = [], emailDate, options = {}) {
    const useRappiDeliverySchedule = Boolean(options?.useRappiDeliverySchedule);
    const allowLlmFallback = options?.allowLlmFallback !== false;
    const sanitizedCandidates = Array.from(new Set(
        (Array.isArray(addressCandidates) ? addressCandidates : [])
            .map((address) => String(address || '').trim())
            .filter(Boolean)
    ));

    if (sanitizedCandidates.length === 0) {
        return null;
    }

    const rawRows = await getAllClients();
    const csvRows = rawRows.map(normalizeClientRecord);

    const rankedMatches = [];
    for (const row of csvRows) {
        const rowAddress = resolveClientDispatchAddress(row);
        if (!rowAddress) {
            continue;
        }
        for (const candidate of sanitizedCandidates) {
            const score = calculateAddressMatchScore(rowAddress, candidate);
            rankedMatches.push({
                score,
                row,
                rowAddress,
                requestedAddress: candidate
            });
        }
    }

    if (rankedMatches.length === 0) {
        return null;
    }

    rankedMatches.sort((a, b) => Number(b.score) - Number(a.score));
    const bestDeterministic = rankedMatches[0] || null;
    let selectedMatch = bestDeterministic && bestDeterministic.score >= ADDRESS_MATCH_MIN_SCORE
        ? bestDeterministic
        : null;
    let matchMethod = selectedMatch ? 'deterministic' : null;
    let llmMetadata = null;

    if (!selectedMatch && allowLlmFallback && bestDeterministic && bestDeterministic.score >= ADDRESS_MATCH_LLM_FALLBACK_MIN_SCORE) {
        const rankedForRequestedAddress = rankedMatches
            .filter((match) => match.requestedAddress === bestDeterministic.requestedAddress);
        const fallbackResult = await resolveAddressMatchWithLlmFallback({
            rankedMatches: rankedForRequestedAddress.length > 0 ? rankedForRequestedAddress : rankedMatches,
            requestedAddress: bestDeterministic.requestedAddress
        });
        if (fallbackResult?.match) {
            selectedMatch = fallbackResult.match;
            matchMethod = 'llm_fallback';
            llmMetadata = fallbackResult.llm || null;
        }
    }

    if (!selectedMatch) {
        return null;
    }

    const selectedRow = { ...selectedMatch.row };
    const deliveryDay = useRappiDeliverySchedule
        ? findRappiDeliveryDayByComuna(
            selectedRow['Comuna Despacho'],
            emailDate,
            getRegionFromClientRecord(selectedRow)
        )
        : findDeliveryDayByComuna(
            selectedRow['Comuna Despacho'],
            emailDate,
            getRegionFromClientRecord(selectedRow)
        );

    if (deliveryDay != null) {
        selectedRow.deliveryDay = String(deliveryDay);
    }

    return {
        data: selectedRow,
        score: selectedMatch.score,
        requestedAddress: selectedMatch.requestedAddress,
        matchedAddress: selectedMatch.rowAddress,
        method: matchMethod,
        llmConfidence: llmMetadata ? Number(llmMetadata.confidence || 0) : null,
        llmReason: llmMetadata ? String(llmMetadata.reason || '').trim() || null : null
    };
}

function searchByAddress(data, address) {
    let bestMatch = null;
    let highestScore = 0;

    data.forEach((item) => {
        const currentAddress = item['direccion despacho'] || '';
        const score = calculateSimilarity(currentAddress, address);

        if (score > highestScore) {
            highestScore = score;
            bestMatch = item;
        }
    });

    return bestMatch;
}

function calculateSimilarity(str1, str2) {
    // Simple similarity calculation using common substring length
    const commonLength = [...str1].filter((char, index) => str2[index] === char).length;
    return commonLength / Math.max(str1.length, str2.length);
}

function getRegionFromClientRecord(record) {
    if (!record || typeof record !== 'object') {
        return '';
    }
    return (
        record['Region Despacho'] ||
        record['Region Despacho'] ||
        record['Region Despacho'] ||
        record.region ||
        ''
    );
}

//integracion con chat gpt

async function integrateWithChatGPT(addresses, targetAddress) {
    const normalizedTarget = String(targetAddress || '').trim();
    if (!Array.isArray(addresses) || addresses.length === 0 || !normalizedTarget) {
        return [];
    }

    const prompt = [
        'Analiza direcciones chilenas y busca la mejor coincidencia para una direccion objetivo.',
        'Recibiras un arreglo de candidatos con formato { index, direccion }.',
        'Devuelve SOLO JSON valido (sin markdown ni texto adicional).',
        'Formato de salida obligatorio:',
        '[{ "index": number, "match": true, "confidence": number, "reason": "string" }]',
        'Si no hay coincidencia suficientemente confiable, devuelve []',
        'Reglas:',
        '- Penaliza fuerte cuando los numeros de direccion no coinciden.',
        '- Si la direccion objetivo no tiene numero y el candidato si lo tiene, no superes confidence 74.',
        '- confidence debe ser 0-100.',
        `Direccion objetivo: "${normalizedTarget}"`,
        `Candidatos: ${JSON.stringify(addresses)}`
    ].join('\n');

    let lastError = null;
    for (const model of ADDRESS_MATCH_GPT_MODELS) {
        try {
            const response = await client.responses.create({
                model,
                input: prompt
            });

            const outputText = String(response?.output_text || '')
                .trim()
                .replace(/```json|```/g, '');
            const parsed = JSON.parse(outputText);
            if (!Array.isArray(parsed)) {
                return [];
            }

            return parsed
                .map((item) => {
                    const confidenceValue = Number(item?.confidence);
                    const confidence = Number.isFinite(confidenceValue)
                        ? Math.max(0, Math.min(100, confidenceValue))
                        : 0;
                    return {
                        index: Number(item?.index),
                        match: item?.match === true,
                        confidence,
                        reason: String(item?.reason || '').trim(),
                        model
                    };
                })
                .filter((item) => Number.isFinite(item.index) && item.match === true);
        } catch (error) {
            lastError = error;
        }
    }

    if (lastError) {
        console.error('Error integrating with GPT address matcher:', lastError?.message || lastError);
    }
    return [];
}

async function readCSV_private(rutToSearch, address, boxPrice, isDelivery, emailDate, options = {}) {
    const useRappiDeliverySchedule = Boolean(options?.useRappiDeliverySchedule);
    console.log(`RUT to search: ${rutToSearch}`);
    console.log(`address to search: ${address}`);
    const normalizedRut = normalizeRut(rutToSearch);
    console.log(`RUT normalizado: ${normalizedRut}`);

    try {
        const rawResults = await getClientsByRut(normalizedRut);
        const results = rawResults.map(normalizeClientRecord);

        console.log("***********************************************");
        console.log("results", results);

        if (results.length == 0) {
            return {
                data: [],
                length: results.length,
                address: address ? true : false,
                message: "Cliente no encontrado en base de clientes",
                boxPriceIsEqual: false
            };
        }

        console.log("1");

        if (results.length == 1) {
            console.log("results[0]", results[0]['Comuna Despacho'], emailDate);
            const deliveryDay = useRappiDeliverySchedule
                ? findRappiDeliveryDayByComuna(results[0]['Comuna Despacho'], emailDate, getRegionFromClientRecord(results[0]))
                : findDeliveryDayByComuna(results[0]['Comuna Despacho'], emailDate, getRegionFromClientRecord(results[0]));
            if (deliveryDay != null) {
                results[0]['deliveryDay'] = `${deliveryDay}`;
            } else {
                const dispatchAddr = String(resolveClientDispatchAddress(results[0]) || '').toLowerCase();
                results[0]['deliveryDay'] = dispatchAddr === 'retiro'
                    ? moment().add(1, 'days').format('YYYY-MM-DD')
                    : '';
            }
            return {
                data: results[0],
                length: results.length,
                address: true,
                message: "Cliente encontrado en base de clientes",
                boxPriceIsEqual: boxPrice == results[0]['Precio Caja'] ? true : false
            };
        }

        console.log("2");

        if (!address) {
            const first = results[0];
            Object.keys(first).forEach(k => { const nk = k.toLowerCase().normalize('NFD').replace(/\p{Diacritic}/gu, ''); if (nk.includes('direccion') && nk.includes('despacho')) first[k] = ''; });
            return {
                data: first,
                length: results.length,
                address: false,
                message: "Cliente no encontrado en base de clientes por falta de direccion",
                boxPriceIsEqual: false
            };
        }

        console.log("3");

        if (isDelivery == false && results.length > 1) {
            results[0]['deliveryDay'] = "";
            return {
                data: results[0],
                length: results.length,
                address: true,
                message: "No se puede encontrar coincidencias por falta de direccion",
                boxPriceIsEqual: boxPrice == results[0]['Precio Caja'] ? true : false
            };
        }

        let bestDeterministicMatch = null;
        for (let index = 0; index < results.length; index += 1) {
            const currentAddress = resolveClientDispatchAddress(results[index]);
            if (!currentAddress) continue;
            const score = calculateAddressMatchScore(currentAddress, address);
            if (!bestDeterministicMatch || score > bestDeterministicMatch.score) {
                bestDeterministicMatch = { score, index };
            }
        }

        if (bestDeterministicMatch && bestDeterministicMatch.score >= ADDRESS_MATCH_MIN_SCORE) {
            const foundDeterministic = results[bestDeterministicMatch.index];
            const deliveryDay = useRappiDeliverySchedule
                ? findRappiDeliveryDayByComuna(foundDeterministic['Comuna Despacho'], emailDate, getRegionFromClientRecord(foundDeterministic))
                : findDeliveryDayByComuna(foundDeterministic['Comuna Despacho'], emailDate, getRegionFromClientRecord(foundDeterministic));
            foundDeterministic.deliveryDay = deliveryDay != null ? `${deliveryDay}` : "";
            return {
                data: foundDeterministic,
                length: 1,
                address: true,
                message: "Se encontro una coincidencia deterministica",
                boxPriceIsEqual: boxPrice == foundDeterministic['Precio Caja'] ? true : false,
                matchConfidence: Math.round(bestDeterministicMatch.score * 100),
                matchMinConfidence: ADDRESS_MATCH_MIN_CONFIDENCE
            };
        }

        const clientData = results.map((item, index) => ({
            index,
            direccion: resolveClientDispatchAddress(item),
        }));

        const gptResponse = await integrateWithChatGPT(clientData, address);
        console.log({ gptResponse });

        if (gptResponse.length == 0) {
            return {
                data: gptResponse,
                length: clientData.length,
                address: address ? true : false,
                boxPriceIsEqual: false
            };
        }

        const matched = gptResponse
            .filter((item) => item.match === true)
            .sort((a, b) => Number(b?.confidence || 0) - Number(a?.confidence || 0))[0];
        const matchConfidence = Number(matched?.confidence || 0);
        const found = results.find((result, index) => index == matched?.index);

        if (!found || matchConfidence < ADDRESS_MATCH_MIN_CONFIDENCE) {
            console.log("no se encontro nada");
            return {
                data: found,
                length: found ? [found].length : 0,
                address: address ? true : false,
                message: !found
                    ? "Direccion no encontrada en base de clientes"
                    : `Direccion descartada por baja confianza (${matchConfidence} < ${ADDRESS_MATCH_MIN_CONFIDENCE})`,
                boxPriceIsEqual: false,
                matchConfidence,
                matchMinConfidence: ADDRESS_MATCH_MIN_CONFIDENCE
            };
        }

        console.log("Se encontro una coincidencia");
        console.log("found", found['Comuna Despacho'], emailDate);
        const deliveryDay = useRappiDeliverySchedule
            ? findRappiDeliveryDayByComuna(found['Comuna Despacho'], emailDate, getRegionFromClientRecord(found))
            : findDeliveryDayByComuna(found['Comuna Despacho'], emailDate, getRegionFromClientRecord(found));
        found['deliveryDay'] = deliveryDay != null ? `${deliveryDay}` : "";
        return {
            data: found,
            length: [found].length,
            address: true,
            message: "Se encontro una coincidencia",
            boxPriceIsEqual: boxPrice == found['Precio Caja'] ? true : false,
            matchConfidence,
            matchMinConfidence: ADDRESS_MATCH_MIN_CONFIDENCE
        };
    } catch (error) {
        console.error('Error en readCSV_private:', error);
        return { success: false, error: 'Error fetching client data', details: error };
    }
}

/**
 * POST /helpers/sync-knowledgebase
 * Trigger manual de sincronización Sheet → MongoDB.
 * Protegido por Bearer token (SHEETS_SYNC_SECRET en .env).
 * Si SHEETS_SYNC_SECRET no está definido, no requiere auth (útil en dev).
 */
async function syncKnowledgebaseHandler(req, res) {

 

    const secret = process.env.SHEETS_SYNC_SECRET;
    if (secret) {
        const authHeader = req.headers['authorization'] || '';
        const token = authHeader.replace(/^Bearer\s+/i, '').trim();
        if (token !== secret) {
            return res.status(401).json({ error: 'No autorizado' });
        }
    }

    try {
        const force = req.query?.force === 'true' || req.body?.force === true;
        const { syncKnowledgebase } = await import('../services/sheetsSyncService.js');
        const stats = await syncKnowledgebase({ force });
        return res.status(200).json({ success: true, ...stats });
    } catch (error) {
        console.error('[syncKnowledgebaseHandler] Error:', error.message);
        return res.status(500).json({ success: false, error: error.message });
    }
}

async function preflightSyncKnowledgebaseHandler(req, res) {
    const secret = process.env.SHEETS_SYNC_SECRET;
    if (secret) {
        const authHeader = req.headers['authorization'] || '';
        const token = authHeader.replace(/^Bearer\s+/i, '').trim();
        if (token !== secret) {
            return res.status(401).json({ error: 'No autorizado' });
        }
    }

    try {
        const { preflightSyncKnowledgebase } = await import('../services/sheetsSyncService.js');
        const report = await preflightSyncKnowledgebase();
        return res.status(report.allPassed ? 200 : 400).json({ success: report.allPassed, ...report });
    } catch (error) {
        console.error('[preflightSyncKnowledgebaseHandler] Error:', error.message);
        return res.status(500).json({ success: false, error: error.message });
    }
}

export {
    readCSV,
    readEmailBody,
    readEmailBodyFromGmail,
    readManualOcBatchDedup,
    readManualOcExtractDate,
    readManualOcPreview,
    readManualOcDispatchPreview,
    readManualOcSubmit,
    syncKnowledgebaseHandler,
    preflightSyncKnowledgebaseHandler
};
