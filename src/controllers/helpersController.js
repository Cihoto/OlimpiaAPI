import fs from 'fs';
import path from 'path';
import { createHash, randomUUID } from 'crypto';
import csvParser from 'csv-parser';
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
    extractPedidosYaOrderNumber
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
    appendManualOcTimeline
} from '../services/mongoManualOcRegistry.js';
import {
    buildGmailClient,
    decodeBase64Url,
    extractEmailAddress,
    extractEmailText,
    findExcelAttachments,
    findPdfAttachments,
    headersToMap
} from '../utils/Google/gmail.js';
const client = new OpenAI();
const KEY_LOGISTICS_BLOCKED_RUTS = new Set(['77.419.327-8', '96.930.440-6']);
const KEY_LOGISTICS_FIXED_RUT = '96.930.440-6';
const KEY_LOGISTICS_SENDER = 'fax@keylogistics.cl';
const PEDIDOS_YA_SENDER = 'compras.marketds@pedidosya.com';
const RAPPI_TURBO_SENDER = 'tomas.bravo@rappi.com';
const MAKE_MANUAL_OC_WEBHOOK_URL = process.env.MAKE_MANUAL_OC_WEBHOOK_URL || '';

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

const MANUAL_OC_DETAIL_MAPPING = Object.freeze([
    Object.freeze({ quantityKey: 'Pedido_Cantidad_Amargo', code: '17798147780069', priceBucket: '150' }),
    Object.freeze({ quantityKey: 'Pedido_Cantidad_Leche', code: '17798147780052', priceBucket: '150' }),
    Object.freeze({ quantityKey: 'Pedido_Cantidad_Pink', code: '70724043633542', priceBucket: '150' }),
    Object.freeze({ quantityKey: 'Pedido_Cantidad_Pink_90g', code: '70724043633549', priceBucket: '90' }),
    Object.freeze({ quantityKey: 'Pedido_Cantidad_Leche_90g', code: '70724043633550', priceBucket: '90' }),
    Object.freeze({ quantityKey: 'Pedido_Cantidad_Free', code: '70724043633551', priceBucket: 'free' })
]);

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
        'Dirección Facturación': 'Pio XI 1290',
        'Dirección Despacho': 'av lo espejo 01740, Bodega 3',
        'Comuna Despacho': 'San Bernardo',
        'Región Despacho': 'Santiago',
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
        'Dirección Facturación': 'Pio XI 1290',
        'Dirección Despacho': 'av lo espejo 01740, Bodega 3',
        'Comuna Despacho': 'San Bernardo',
        'Región Despacho': 'Santiago',
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
        'Dirección Facturación': 'Pio XI 1290',
        'Dirección Despacho': 'lago riñihue 2319',
        'Comuna Despacho': 'San Bernardo',
        'Región Despacho': 'Santiago',
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
        'Dirección Facturación': 'Avenida Lo Espejo 01740, Bodega 3, San Bernardo',
        'Dirección Despacho': 'Avenida Lo Espejo 01740, Bodega 3',
        'Comuna Despacho': 'San Bernardo',
        'Región Despacho': 'Santiago',
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
    } else if (String(data['Dirección Despacho'] || '').toLowerCase() === 'retiro') {
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
        if (k.includes('direccion') && k.includes('despacho')) return 'Dirección Despacho';
        if (k.includes('direccion') && k.includes('facturacion')) return 'Dirección Facturación';
        if (k.includes('comuna')) return 'Comuna Despacho';
        if (k.includes('region') || (k.includes('region') && k.includes('despacho')) || (k.includes('reg') && k.includes('despacho'))) return 'Región Despacho';
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
                    first['Dirección Despacho'] = "";
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
                        direccion: item['Dirección Despacho'],
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

                const matched = gptResponse.find((item) => item.match === true);
                const found = results.find((result, index) => {
                    return index == (matched.index)
                });

                if (!found) {
                    console.log("no se encontro nada")
                    res.status(200).json({
                        data: found,
                        length: [found].length,
                        address: address ? true : false,
                        message: "No se encontro nada"
                    })
                    return;
                }
                // If a match is found, return the matched address

                res.status(200).json({
                    data: found,
                    length: [found].length,
                    address: true,
                    message: "Se encontro una coincidencia",
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
        const hasSku = labels.some((label) => label === 'n sku' || label === 'sku');
        const hasDescription = labels.some((label) => label === 'descripcion');
        const hasQty = labels.some((label) => label.includes('cantidad cajas') || label.includes('cantidad unidades'));
        if (hasSku && hasDescription && hasQty) {
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

        if (columnMap.sku === undefined && (label === 'n sku' || label === 'sku')) {
            columnMap.sku = colIndex;
            return;
        }
        if (columnMap.ean === undefined && label === 'ean') {
            columnMap.ean = colIndex;
            return;
        }
        if (columnMap.internalCode === undefined && label.includes('codigo interno proveedor')) {
            columnMap.internalCode = colIndex;
            return;
        }
        if (columnMap.description === undefined && label === 'descripcion') {
            columnMap.description = colIndex;
            return;
        }
        if (columnMap.quantityBoxes === undefined && label.includes('cantidad cajas')) {
            columnMap.quantityBoxes = colIndex;
            return;
        }
        if (columnMap.quantityUnits === undefined && label.includes('cantidad unidades')) {
            columnMap.quantityUnits = colIndex;
            return;
        }
        if (columnMap.unitCost === undefined && (label.includes('costo s unidad') || label.includes('costo unidad'))) {
            columnMap.unitCost = colIndex;
            return;
        }
        if (columnMap.costExclIva === undefined && label.includes('costo excl iva')) {
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
    const maxPreviewCols = Number.parseInt(process.env.MANUAL_OC_PREVIEW_COLS || '12', 10);
    const maxReplicaRows = Number.parseInt(process.env.MANUAL_OC_REPLICA_MAX_ROWS || '140', 10);
    const maxReplicaCols = Number.parseInt(process.env.MANUAL_OC_REPLICA_MAX_COLS || '16', 10);
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
            values: row.slice(0, maxPreviewCols).map((cell, colIndex) => formatExcelPreviewCell(row, colIndex))
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
        .map((row) => row.slice(0, maxReplicaCols).map((cell, colIndex) => formatExcelPreviewCell(row, colIndex)));

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
    uploadedBy
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
        manualOc: {
            id: manualOcId,
            sourceClientCode: profile.sourceClientCode,
            sourceClientName: profile.sourceClientName,
            parserProfile: profile.parserProfile
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
        'RegiÃ³n',
        'RegiÃƒÂ³n',
        'RegiÃƒÆ’Ã‚Â³n'
    ]) || getObjectValueByKeys(clientData, [
        'region',
        'RegiÃ³n Despacho',
        'RegiÃƒÂ³n Despacho',
        'RegiÃƒÆ’Ã‚Â³n Despacho',
        'Región Despacho'
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
        'RegiÃ³n',
        'RegiÃƒÂ³n',
        'RegiÃƒÆ’Ã‚Â³n'
    ]) || getObjectValueByKeys(clientData, [
        'region',
        'RegiÃ³n Despacho',
        'RegiÃƒÂ³n Despacho',
        'RegiÃƒÆ’Ã‚Â³n Despacho',
        'Región Despacho'
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
        'Región Despacho',
        'RegiÃ³n Despacho'
    ]);
    const gloss = getObjectValueByKeys(emailData, [
        'Direccion_despacho',
        'Dirección_despacho'
    ]) || getObjectValueByKeys(clientData, [
        'Dirección Despacho',
        'DirecciÃ³n Despacho'
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
    fileMeta
}) {
    if (!MAKE_MANUAL_OC_WEBHOOK_URL) {
        return {
            delivered: false,
            skipped: true,
            reason: 'missing_make_webhook_url'
        };
    }

    const merged = mergedResult?.merged || null;
    const deliveryReservation = mergedResult?.deliveryReservation || null;
    const emailData = merged?.EmailData || null;
    const clientData = merged?.ClientData?.data || merged?.ClientData || null;
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

    const makePayload = {
        source: 'manual_oc',
        mode: 'TEST_ONLY',
        testMode: true,
        preventBilling: true,
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
        merged,
        deliveryReservation
    };

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
        } = JSON.parse(sanitizedEmailBody); // Parse the sanitized email body

        console.log(JSON.parse(sanitizedEmailBody));

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

        const missingFields = requiredFields.filter(field => !(field in JSON.parse(sanitizedEmailBody)));

        if (missingFields.length > 0) {
            console.log("Invalid request, missing fields:", missingFields);
            return res.status(400).json({ error: 'Invalid request body' });
        }

        let attachedPrompt = ""
        let OC = ""
        if (emailAttached !== "") {
            attachedPrompt = `y el texto que hemos extraido desde un PDF adjunto que trae la orden de compra con el pedido: "${emailAttached}". `
        }

        const systemPrompt = `DevuÃ©lveme exclusivamente un JSON vÃ¡lido, sin explicaciones ni texto adicional.
        La respuesta debe comenzar directamente con [ y terminar con ].
        No incluyas ningÃºn texto antes o despuÃ©s del JSON.
        No uses formato Markdown. 
        No expliques lo que estÃ¡s haciendo.
        Tu respuesta debe ser solamente el JSON. Nada mÃ¡s.;`;

        // const userPrompt = `Eres un bot que analiza pedidos para FranuÃ­, empresa que comercializa frambuesas baÃ±adas en chocolate. FranuÃ­ maneja solamente 3 productos
        //     Frambuesas baÃ±adas en chocolate amargo
        //     Frambuesas baÃ±adas en chocolate de leche
        //     Frambuesas baÃ±adas en chocolate pink

        //     Debes analizar el texto del body del correo ${emailBody}, el asunto ${emailSubject} y cualquier informaciÃ³n contenida en ${attachedPrompt} para extraer los datos relevantes y guardarlos en variables

        //     Nuestro negocio se llama Olimpia SPA y nuestro rut es 77.419.327-8. Ninguna variable extraÃ­da debe contener la palabra Olimpia ni nuestro RUT

        //     Importante el campo Rut es obligatorio y prioritario. Si no se encuentra, la ejecuciÃ³n es invÃ¡lida
        //     Debes buscar el primer RUT que no sea el de Olimpia SPA 77.419.327-8
        //     Los formatos posibles son
        //     xx.xxx.xxx-x
        //     xxx.xxx.xxx-x
        //     xxxxxxxx-x
        //     El RUT puede encontrarse en cualquier parte del correo o asunto
        //     No devuelvas el RUT si es igual a 77.419.327-8 y continÃºa buscando hasta encontrar uno vÃ¡lido
        //     Si no encuentras ningÃºn otro RUT vÃ¡lido, devuelve null

        //     Debes extraer los siguientes datos
        //     Razon_social contiene la razÃ³n social del cliente
        //     Direccion_despacho direcciÃ³n a la cual se enviarÃ¡n los productos. Si no la encuentras, devuelve null
        //     Comuna comuna de despacho. Si no la encuentras, devuelve null
        //     Rut ver reglas anteriores
        //     Pedido_Cantidad_Pink cantidad de cajas de chocolate pink. Si no existe, devuelve 0
        //     Pedido_Cantidad_Amargo: cantidad de cajas de chocolate amargo. Si no existe, devuelve 0
        //     Pedido_Cantidad_Leche: cantidad de cajas de chocolate de leche. Si no existe, devuelve 0
        //     Pedido_PrecioTotal_Pink: devuelve 0
        //     Pedido_PrecioTotal_Amargo monto total del pedido de chocolate amargo. Si no existe, devuelve 0
        //     Pedido_PrecioTotal_Leche monto total del pedido de chocolate de leche. Si no existe, devuelve 0
        //     Orden_de_Compra nÃºmero de orden de compra. Si no existe, devuelve null
        //     Monto neto tambiÃ©n llamado subtotal. Si no existe, devuelve 0
        //     Iva monto del impuesto. Si no existe, devuelve 0
        //     Total monto total del pedido incluyendo impuestos. Si no existe, devuelve 0
        //     Sender_Email correo electrÃ³nico del remitente del mensaje
        //     precio_caja precio de la caja de chocolate pink amargo o leche. Si no existe, devuelve 0
        //     URL_ADDRESS direcciÃ³n de despacho codificada en formato URL lista para usarse en una peticiÃ³n HTTP GET. No devuelvas nada mÃ¡s que la cadena codificada sin explicaciones ni comillas
        //     PaymentMethod
        //     method en caso de hacer referencia a un cheque devolver letra C en caso contrario devuelve vacÃ­o
        //     paymentsDays nÃºmero de dÃ­as de pago si se menciona. En caso contrario devuelve vacÃ­o
        //     isDelivery en caso de que el pedido sea para delivery devuelve true si no es para delivery devuelve false

        //     Reglas para campo Razon_social
        //     Puede estar en el cuerpo del correo o en el asunto
        //     En caso de no haber una indicaciÃ³n clara puede estar mencionada como sucursal local o cliente

        //     Reglas para Direccion_despacho
        //     Puede estar en el cuerpo del correo o en el asunto
        //     Debe incluir calle y comuna
        //     Si no se menciona direcciÃ³n especÃ­fica puede estar indicada como sucursal o local
        //     Si el pedido es para retiro reemplaza este valor por la palabra RETIRO

        //     Reglas para precio_caja
        //     El precio de la caja ronda entre los 60000 y 80000 pesos
        //     Debe ser el mismo para pink amargo y leche
        //     Si no se encuentra en el texto devuelve 0

        //     Reglas para isDelivery
        //     Si el pedido es para retiro en sucursal devolver false
        //     Si no se menciona retiro explÃ­citamente devolver true
        //     Ejemplos de retiro
        //     te quiero hacer un pedido para retirar este viernes
        //     pedido con retiro
        //     En caso de duda devolver true por defecto
        // `

        const userPrompt =
            `Eres un bot que analiza pedidos para FranuÃ­, empresa que comercializa frambuesas baÃ±adas en chocolate.

FranuÃ­ maneja los siguientes productos:

=== PRODUCTOS DE 150 GRAMOS (24 unidades por caja) ===
- Frambuesas baÃ±adas en chocolate amargo
- Frambuesas baÃ±adas en chocolate de leche
- Frambuesas baÃ±adas en chocolate pink
- FranuÃ­ Chocolate Free (sin azÃºcar)

=== PRODUCTOS DE 90 GRAMOS (18 unidades por caja) ===
- Caja Franui Amargo 90 gramos
- Caja Franui Leche 90 gramos
- Caja Franui Pink 90 gramos

IMPORTANTE: Si el producto NO especifica "90g" o "90 gramos", se asume que es el producto de 150 gramos.

Debes analizar el texto del body del correo ${emailBody}, el asunto ${emailSubject} y cualquier informaciÃ³n contenida en ${attachedPrompt} para extraer los datos relevantes y guardarlos en variables

Nuestro negocio se llama Olimpia SPA y nuestro rut es 77.419.327-8. Ninguna variable extraÃ­da debe contener la palabra Olimpia ni nuestro RUT

Importante el campo Rut es obligatorio y prioritario. Si no se encuentra, la ejecuciÃ³n es invÃ¡lida
Debes buscar el primer RUT que no sea el de Olimpia SPA 77.419.327-8
Los formatos posibles son
xx.xxx.xxx-x
xxx.xxx.xxx-x
xxxxxxxx-x
El RUT puede encontrarse en cualquier parte del correo o asunto
No devuelvas el RUT si es igual a 77.419.327-8 y continÃºa buscando hasta encontrar uno vÃ¡lido
Si no encuentras ningÃºn otro RUT vÃ¡lido, devuelve null

Debes extraer los siguientes datos:

=== DATOS DEL CLIENTE ===
Razon_social: contiene la razÃ³n social del cliente
Direccion_despacho: direcciÃ³n PRINCIPAL de despacho. Priorizar la que diga "despacho", "entrega" o "envÃ­o". Si no la encuentras, devuelve null
Direcciones_encontradas: ARRAY con TODAS las direcciones encontradas en el documento (facturaciÃ³n, despacho, entrega, etc). Esto es MUY IMPORTANTE para poder buscar coincidencias. Ejemplo: ["NUEVA LOS LEONES 030 LOCAL 16", "AVDA COSTANERA SUR 2710 PISO 12"]
Comuna: comuna de despacho. Si no la encuentras, devuelve null
Rut: ver reglas anteriores

=== CANTIDADES DE PRODUCTOS 150g (24 unidades por caja) ===
Pedido_Cantidad_Pink: cantidad de cajas de chocolate pink 150g. Si no existe, devuelve 0
Pedido_Cantidad_Amargo: cantidad de cajas de chocolate amargo 150g. Si no existe, devuelve 0
Pedido_Cantidad_Leche: cantidad de cajas de chocolate de leche 150g. Si no existe, devuelve 0
Pedido_Cantidad_Free: cantidad de cajas de FranuÃ­ Chocolate Free (sin azÃºcar) 150g. Si no existe, devuelve 0

=== CANTIDADES DE PRODUCTOS 90g (18 unidades por caja) ===
Pedido_Cantidad_Pink_90g: cantidad de cajas de chocolate pink 90g. Si no existe, devuelve 0
Pedido_Cantidad_Amargo_90g: cantidad de cajas de chocolate amargo 90g. Si no existe, devuelve 0
Pedido_Cantidad_Leche_90g: cantidad de cajas de chocolate de leche 90g. Si no existe, devuelve 0

=== PRECIOS PRODUCTOS 150g ===
Pedido_PrecioTotal_Pink: monto total del pedido de chocolate pink 150g. Si no existe, devuelve 0
Pedido_PrecioTotal_Amargo: monto total del pedido de chocolate amargo 150g. Si no existe, devuelve 0
Pedido_PrecioTotal_Leche: monto total del pedido de chocolate de leche 150g. Si no existe, devuelve 0
Pedido_PrecioTotal_Free: monto total del pedido de FranuÃ­ Chocolate Free 150g. Si no existe, devuelve 0

=== PRECIOS PRODUCTOS 90g ===
Pedido_PrecioTotal_Pink_90g: monto total del pedido de chocolate pink 90g. Si no existe, devuelve 0
Pedido_PrecioTotal_Amargo_90g: monto total del pedido de chocolate amargo 90g. Si no existe, devuelve 0
Pedido_PrecioTotal_Leche_90g: monto total del pedido de chocolate de leche 90g. Si no existe, devuelve 0

=== DATOS DE LA ORDEN ===
Orden_de_Compra: nÃºmero de orden de compra. Si no existe, devuelve null
Monto: neto tambiÃ©n llamado subtotal. Si no existe, devuelve 0
Iva: monto del impuesto. Si no existe, devuelve 0
Total: monto total del pedido incluyendo impuestos. Si no existe, devuelve 0
Sender_Email: correo electrÃ³nico del remitente del mensaje

=== PRECIOS POR CAJA ===
precio_caja: precio de la caja de chocolate pink, amargo o leche 150g. Si no existe, devuelve 0
precio_caja_90g: precio de la caja de productos 90g. Si no existe, devuelve 0
precio_caja_free: precio de la caja de FranuÃ­ Chocolate Free. Si no existe, devuelve 0

URL_ADDRESS: direcciÃ³n de despacho codificada en formato URL lista para usarse en una peticiÃ³n HTTP GET. No devuelvas nada mÃ¡s que la cadena codificada sin explicaciones ni comillas

PaymentMethod:
method: en caso de hacer referencia a un cheque devolver letra C, en caso contrario devuelve vacÃ­o
paymentsDays: nÃºmero de dÃ­as de pago si se menciona. En caso contrario devuelve vacÃ­o

isDelivery: en caso de que el pedido sea para delivery devuelve true, si no es para delivery devuelve false

=== REGLAS ESPECÃFICAS ===

Reglas para campo Razon_social:
Puede estar en el cuerpo del correo o en el asunto
En caso de no haber una indicaciÃ³n clara puede estar mencionada como sucursal local o cliente

Reglas para Direccion_despacho:
Puede estar en el cuerpo del correo o en el asunto
Debe incluir calle y comuna
Si no se menciona direcciÃ³n especÃ­fica puede estar indicada como sucursal o local
Si el pedido es para retiro reemplaza este valor por la palabra RETIRO
PRIORIDAD: Si hay mÃºltiples direcciones, priorizar la que estÃ© etiquetada como "despacho", "entrega" o "envÃ­o" sobre la de "facturaciÃ³n"

Reglas para Direcciones_encontradas:
Debe ser un ARRAY con TODAS las direcciones fÃ­sicas encontradas en el documento
Incluir tanto direcciones de facturaciÃ³n como de despacho
No incluir direcciones de correo electrÃ³nico
No incluir direcciones web/URL
Ejemplo: si el documento dice "DirecciÃ³n: NUEVA LOS LEONES 030" y "DirecciÃ³n: AVDA COSTANERA SUR 2710", devolver ["NUEVA LOS LEONES 030", "AVDA COSTANERA SUR 2710"]
Si solo hay una direcciÃ³n, devolver array con un elemento
Si no hay direcciones, devolver array vacÃ­o []

Reglas para identificar productos de 90g:
Buscar menciones de "90g", "90 gramos", "90gr" en el nombre del producto
Ejemplos: "Franui Leche 90g", "Caja Franui Pink 90 gramos", "Amargo 90g"
Si NO especifica gramos, asumir que es producto de 150g

Reglas para identificar FranuÃ­ Chocolate Free:
Buscar menciones de "Free", "Chocolate Free", "sin azÃºcar"
Ejemplos: "FranuÃ­ Chocolate Free", "Franui Free", "Caja Franui Free"

Reglas para precio_caja (150g):
El precio de la caja ronda entre los 60000 y 80000 pesos
Debe ser el mismo para pink, amargo y leche
Si no se encuentra en el texto devuelve 0

Reglas para precio_caja_90g:
Precio de las cajas de productos de 90 gramos
Si no se encuentra en el texto devuelve 0

Reglas para precio_caja_free:
Precio de las cajas de FranuÃ­ Chocolate Free
Si no se encuentra en el texto devuelve 0

Reglas para isDelivery:
Si el pedido es para retiro en sucursal devolver false
Si no se menciona retiro explÃ­citamente devolver true
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
    "Sender_Email": "valor o vacÃ­o",
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
            String(sender || '').toLowerCase() === RAPPI_TURBO_SENDER;
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

        const injectedQuantities = keyLogisticsData?.quantities || rappiTurboData?.quantities;
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
            : (source === 'gmail'
                ? await analyzeOrderEmailFromGmail(sanitizedEmailBody)
                : await analyzeOrderEmail(sanitizedEmailBody));
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
            validJson.Direccion_despacho = fixedData['Dirección Despacho'];
            validJson.Comuna = fixedData['Comuna Despacho'];
            validJson.Rut = fixedData.RUT;

            const fixedAddresses = [
                fixedData['Dirección Despacho'],
                fixedData['Dirección Facturación']
            ].filter(Boolean);
            const extractedAddresses = Array.isArray(validJson.Direcciones_encontradas)
                ? validJson.Direcciones_encontradas
                : [];
            validJson.Direcciones_encontradas = Array.from(
                new Set([...extractedAddresses, ...fixedAddresses])
            );

            const regionDespacho = fixedData['Región Despacho'];
            const regionNormalized = String(regionDespacho || '').toLowerCase().trim();
            if (regionNormalized === "santiago") {
                keyLogisticsFixedClientData.data['region'] = "RM";
            } else if (regionNormalized === "ohiggins" || regionNormalized === "o'higgins") {
                keyLogisticsFixedClientData.data['region'] = "VI";
            } else if (regionNormalized === "valparaÃ­so" || regionNormalized === "valparaiso") {
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
        
        // Agregar direcciÃ³n principal si existe
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
        
        // Intentar con cada direcciÃ³n hasta encontrar una coincidencia vÃ¡lida
        let clientData = null;
        let direccionUsada = null;
        
        for (const direccion of direccionesAProbar) {
            console.log(`Probando direcciÃ³n: ${direccion}`);
            const resultado = await readCSV_private(
                validJson.Rut,
                direccion,
                validJson.precio_caja,
                validJson.isDelivery,
                emailDate,
                { useRappiDeliverySchedule: isRappiTurboGmail }
            );
            
            // Verificar si encontramos datos vÃ¡lidos (no array vacÃ­o y tiene Región Despacho)
            const regionDespachoTemp = resultado?.data?.['Región Despacho'];
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
        
        // Si no encontramos nada con ninguna direcciÃ³n, usar el resultado del Ãºltimo intento o hacer uno con la principal
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
        console.log("clientData Región Despacho", clientData.data?.['Región Despacho']);
        console.log("DirecciÃ³n usada para match:", direccionUsada);
        console.log("{}{}{}{}{}{}{}{}{}{}{}{}{}{}}{{}}{{}}{}{}{}{}{}{}{}{}{");
        
        // Verificar si clientData.data existe, no es array, y tiene 'Región Despacho' como string vÃ¡lido
        const regionDespacho = clientData?.data?.['Región Despacho'];
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

            // Si no hay datos del cliente vÃ¡lidos, retornar error con info de direcciones probadas
            return res.status(400).json({
                success: false,
                error: 'No se encontrÃ³ coincidencia de direcciÃ³n en la base de clientes',
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
        
        // Si usamos una direcciÃ³n alternativa, actualizar validJson para reflejar la correcta
        if (direccionUsada && direccionUsada !== validJson.Direccion_despacho) {
            console.log(`Actualizando Direccion_despacho de "${validJson.Direccion_despacho}" a "${direccionUsada}"`);
            validJson.Direccion_despacho_original = validJson.Direccion_despacho;
            validJson.Direccion_despacho = direccionUsada;
        }
        
        // Ahora es seguro usar toLowerCase() porque ya validamos que es string no vacÃ­o
        const regionNormalized = regionDespacho.toLowerCase().trim();
        
        if (regionNormalized === "santiago") {
            clientData.data['region'] = "RM";
        } else if (regionNormalized === "ohiggins" || regionNormalized === "o'higgins") {
            clientData.data['region'] = "VI";
        } else if (regionNormalized === "valparaÃ­so" || regionNormalized === "valparaiso") {
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

        const excelAnalysis = extractedFile?.excelPreview?.analysis || null;
        const detectedDateInfoFromText = detectOcDateFromText(extractedFile.text);
        const detectedDateInfo = (detectedDateInfoFromText?.date || !excelAnalysis?.dates?.fechaEmision)
            ? detectedDateInfoFromText
            : {
                date: excelAnalysis.dates.fechaEmision,
                confidence: 'high',
                method: 'excel_field_fecha_emision'
            };
        const manualOcId = randomUUID();
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
            rawPayloadForParser: null,
            excelText: extractedFile.text,
            excelAnalysis,
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
            ocDateDetected: detectedDateInfo.date || null,
            ocDateDetectedConfidence: detectedDateInfo.confidence,
            ocDateDetectionMethod: detectedDateInfo.method,
            excelPreview: extractedFile.excelPreview || null,
            excelAnalysis,
            warnings
        });
    } catch (error) {
        console.error('Error en readManualOcExtractDate:', error);
        return res.status(500).json({
            success: false,
            error: 'No se pudo extraer fecha manual OC',
            details: error?.message || String(error)
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

        if (!/\.xlsx?$/i.test(fileName)) {
            return res.status(400).json({
                success: false,
                error: 'Solo se aceptan archivos Excel (.xlsx/.xls) para este flujo'
            });
        }

        const fileBuffer = decodeFileBase64ToBuffer(fileBase64);
        const fileSha256 = createHash('sha256').update(fileBuffer).digest('hex');
        const excelText = excelBufferToText(fileBuffer);

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
            uploadedBy
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
                fileSha256
            },
            ocDateDetected: detectedDateInfo.date || null,
            ocDateDetectedConfidence: detectedDateInfo.confidence,
            ocDateDetectionMethod: detectedDateInfo.method,
            rawPayloadForParser: payload,
            excelText,
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
            ocDateDetected: detectedDateInfo.date || null,
            ocDateDetectedConfidence: detectedDateInfo.confidence,
            warnings,
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
        uploadedBy: uploadedBy || record.uploadedBy
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

    const merged = parserResponse.body?.merged || {};
    const dispatchContext = {
        emailData: merged?.EmailData || {},
        clientData: merged?.ClientData?.data || merged?.ClientData || {},
        deliveryReservation: parserResponse.body?.deliveryReservation || null,
        contextBuiltAt: new Date().toISOString()
    };

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
                error: 'manualOcId es requerido'
            });
        }

        const record = await findManualOcRecord(manualOcId);
        if (!record) {
            return res.status(404).json({
                success: false,
                error: `No existe registro manual OC (${manualOcId})`
            });
        }

        const profile = getManualOcClientProfile(record.sourceClientCode);
        if (!profile) {
            return res.status(400).json({
                success: false,
                error: `sourceClientCode no soportado: ${record.sourceClientCode}`
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
                error: 'ocDateConfirmed invalida. Use formato YYYY-MM-DD o DD/MM/YYYY'
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
                return res.status(422).json({
                    success: false,
                    error: 'No se pudo calcular preview de despacho',
                    parser: contextError.parser
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
                error: 'arrivalDate invalida. Use formato YYYY-MM-DD o DD/MM/YYYY'
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

        await updateManualOcRecord(manualOcId, {
            ocDateConfirmed: confirmedDate,
            arrivalDate: arrivalInfo.date,
            arrivalMeridiem: arrivalInfo.meridiem,
            dispatchPreviewDate,
            dispatchPreviewAt: new Date().toISOString()
        });
        await appendManualOcTimeline(manualOcId, {
            event: 'dispatch_preview_calculated',
            ocDateConfirmed: confirmedDate,
            arrivalDate: arrivalInfo.date,
            arrivalMeridiem: arrivalInfo.meridiem,
            dispatchPreviewDate
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
            dispatchContextFromCache
        });
    } catch (error) {
        console.error('Error en readManualOcDispatchPreview:', error);
        return res.status(500).json({
            success: false,
            error: 'No se pudo calcular preview de despacho manual OC',
            details: error?.message || String(error)
        });
    }
}

async function readManualOcSubmit(req, res) {
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
                error: 'manualOcId es requerido'
            });
        }

        const record = await findManualOcRecord(manualOcId);
        if (!record) {
            return res.status(404).json({
                success: false,
                error: `No existe registro manual OC (${manualOcId})`
            });
        }

        const profile = getManualOcClientProfile(record.sourceClientCode);
        if (!profile) {
            return res.status(400).json({
                success: false,
                error: `sourceClientCode no soportado: ${record.sourceClientCode}`
            });
        }

        const confirmedDate = parseManualDateCandidate(ocDateConfirmed || record.ocDateDetected);
        if (!confirmedDate) {
            return res.status(400).json({
                success: false,
                error: 'ocDateConfirmed invalida. Use formato YYYY-MM-DD o DD/MM/YYYY'
            });
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
                error: 'arrivalDate invalida. Use formato YYYY-MM-DD o DD/MM/YYYY'
            });
        }

        const emailDateToUse = arrivalInfo.dateTimeIso;
        const payload = buildManualReadEmailPayload({
            manualOcId,
            profile,
            fileName: record?.fileMeta?.fileName || 'manual_oc.xlsx',
            excelText: record.excelText || '',
            emailDate: emailDateToUse,
            uploadedBy: uploadedBy || record.uploadedBy
        });

        await updateManualOcRecord(manualOcId, {
            status: 'submit_processing',
            ocDateConfirmed: confirmedDate,
            arrivalDate: arrivalInfo.date,
            arrivalMeridiem: arrivalInfo.meridiem,
            submitRequestedBy: uploadedBy || record.uploadedBy
        });
        await appendManualOcTimeline(manualOcId, {
            event: 'submit_requested',
            ocDateConfirmed: confirmedDate,
            arrivalDate: arrivalInfo.date,
            arrivalMeridiem: arrivalInfo.meridiem
        });

        const parserResponse = await runReadEmailBodyPayload(payload);
        const parserSuccess = parserResponse.status >= 200 && parserResponse.status < 300;

        if (!parserSuccess) {
            await updateManualOcRecord(manualOcId, {
                status: 'submit_failed_parser',
                submitParserStatusCode: parserResponse.status,
                submitParserResponseBody: parserResponse.body
            });
            await appendManualOcTimeline(manualOcId, {
                event: 'submit_failed_parser',
                statusCode: parserResponse.status
            });

            return res.status(422).json({
                success: false,
                error: 'El parser no pudo construir merged valido para submit',
                parser: {
                    status: parserResponse.status,
                    body: parserResponse.body
                }
            });
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
            fileMeta: record.fileMeta || null
        });

        const finalStatus = makeResult.delivered
            ? 'submitted_to_make'
            : (makeResult.skipped ? 'submit_skipped_make' : 'submit_failed_make');

        await updateManualOcRecord(manualOcId, {
            status: finalStatus,
            submitParserStatusCode: parserResponse.status,
            submitParserResponseBody: parserResponse.body,
            makeResult
        });
        await appendManualOcTimeline(manualOcId, {
            event: finalStatus,
            makeStatus: makeResult.status || null,
            makeDelivered: makeResult.delivered === true
        });

        const statusCode = makeResult.delivered || makeResult.skipped ? 200 : 502;
        return res.status(statusCode).json({
            success: makeResult.delivered || makeResult.skipped,
            manualOcId,
            sourceClientCode: profile.sourceClientCode,
            parserProfile: profile.parserProfile,
            ocDateDetected: record.ocDateDetected || null,
            ocDateConfirmed: confirmedDate,
            arrivalDate: arrivalInfo.date,
            arrivalMeridiem: arrivalInfo.meridiem,
            arrivalDateTime: arrivalInfo.dateTimeIso,
            parser: {
                status: parserResponse.status,
                body: parserResponse.body
            },
            make: makeResult
        });
    } catch (error) {
        console.error('Error en readManualOcSubmit:', error);
        return res.status(500).json({
            success: false,
            error: 'No se pudo completar submit manual OC',
            details: error?.message || String(error)
        });
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
        const allowedSenders = new Set([
            PEDIDOS_YA_SENDER,
            KEY_LOGISTICS_SENDER,
            RAPPI_TURBO_SENDER
        ]);

        if (!allowedSenders.has(sender)) {
            return res.status(403).json({
                success: false,
                error: 'Emisor no permitido',
                sender,
                allowedSenders: Array.from(allowedSenders)
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

        if (sender === RAPPI_TURBO_SENDER) {
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
        record['Región Despacho'] ||
        record['RegiÃ³n Despacho'] ||
        record['RegiÃƒÂ³n Despacho'] ||
        record.region ||
        ''
    );
}

//integracion con chat gpt

async function integrateWithChatGPT(addresses, targetAddress) {

    const prompt = `Busca dentro de este arreglo ${JSON.stringify(addresses)} la mejor coincidencia para la direcciÃ³n "${targetAddress}".
    En caso de encontrar una coincidencia, devolver un array JSON con el objeto que contenga la direcciÃ³n agregando "match": true.
    En caso de no encontrar coincidencias, devolver un array vacio. En caso de tener coincidencias, devolver solo un elemento.
    [no prose] [Output only JSON]`;

    const response = await client.responses.create({
        model: "gpt-4o-mini",
        input: prompt
    });

    try {
        const sanitizedOutput = response.output_text.trim().replace(/```json|```/g, '').replace(/\n/g, '').replace(/\\/g, '');
        const validJson = JSON.parse(sanitizedOutput);
        return validJson; // Return the parsed JSON as an array
    } catch (error) {
        console.error('Error parsing JSON from GPT response:', error);
        return []; // Return an empty array in case of error
    }
}

async function readCSV_private(rutToSearch, address, boxPrice, isDelivery, emailDate, options = {}) {
    const useRappiDeliverySchedule = Boolean(options?.useRappiDeliverySchedule);
    const results = [];
    console.log(`RUT to search: ${rutToSearch}`); // Log the RUT to search
    console.log(`address to search: ${address}`); // Log the address to search
    const normalizedRut = normalizeRut(rutToSearch); // Normalize the RUT
    console.log(`RUT to search: ${normalizedRut}`); // Log the RUT to search

    return new Promise((resolve, reject) => {
        try {
            fs.createReadStream(CSV)
                .pipe(csvParser())
                .on('data', (data) => {
                    if (data.RUT.toLowerCase() == normalizedRut.toLocaleLowerCase()) {
                        results.push(data);
                    } // Collect all rows
                })
                .on('end', async () => {
                    console.log("***********************************************")
                    console.log("results", results);

                    // Normalize all records' keys and values using helper
                    for (let i = 0; i < results.length; i++) {
                        results[i] = normalizeClientRecord(results[i]);
                    }

                    // return results;

                    if (results.length == 0) {
                        resolve({
                            data: [],
                            length: results.length,
                            address: address ? true : false,
                            message: "Cliente no encontrado en base de clientes",
                            boxPriceIsEqual: false
                        });
                        return;
                    }

                    console.log("1")

                    if (results.length == 1) {
                        console.log("results[0]", results[0]['Comuna Despacho'], emailDate);


                        const deliveryDay = useRappiDeliverySchedule
                            ? findRappiDeliveryDayByComuna(
                                results[0]['Comuna Despacho'],
                                emailDate,
                                getRegionFromClientRecord(results[0])
                            )
                            : findDeliveryDayByComuna(
                                results[0]['Comuna Despacho'],
                                emailDate,
                                getRegionFromClientRecord(results[0])
                            );
                        if (deliveryDay != null) {
                            results[0]['deliveryDay'] = `${deliveryDay}`;
                        } else {
                            if (results[0]['Dirección Despacho'].toLowerCase() == "retiro") {
                                results[0]['deliveryDay'] = moment().add(1, 'days').format('YYYY-MM-DD');
                            } else {
                                results[0]['deliveryDay'] = "";
                            }
                        }
                        resolve({
                            data: results[0],
                            length: results.length,
                            address: true,
                            message: "Cliente encontrado en base de clientes",
                            boxPriceIsEqual: boxPrice == results[0]['Precio Caja'] ? true : false
                        });
                        return;
                    }
                    console.log("2")

                    if (!address) {
                        const first = results[0];
                        first['Dirección Despacho'] = "";
                        resolve({
                            data: first,
                            length: results.length,
                            address: false,
                            message: "Cliente no encontrado en base de clientes por falta de direcciÃ³n",
                            boxPriceIsEqual: false
                        });
                        return;
                    }
                    console.log("3")
                    if (isDelivery == false && results.length > 1) {
                        results[0]['deliveryDay'] = "";
                        resolve({
                            data: results[0],
                            length: results.length,
                            address: true,
                            message: "No se puede encontrar coincidencias por falta de direcciÃ³n",
                            boxPriceIsEqual: boxPrice == results[0]['Precio Caja'] ? true : false
                        });
                    }

                    // Map results array for GPT token limitation
                    const clientData = results.map((item, index) => {
                        return {
                            index: index,
                            direccion: item['Dirección Despacho'],
                        };
                    });

                    const gptResponse = await integrateWithChatGPT(clientData, address); // Integrate with ChatGPT
                    console.log({ gptResponse });

                    if (gptResponse.length == 0) {
                        resolve({
                            data: gptResponse,
                            length: clientData.length,
                            address: address ? true : false,
                            boxPriceIsEqual: false
                        });
                        return;
                    }

                    const matched = gptResponse.find((item) => item.match === true);
                    const found = results.find((result, index) => {
                        return index == matched.index;
                    });

                    if (!found) {
                        console.log("no se encontro nada");
                        resolve({
                            data: found,
                            length: [found].length,
                            address: address ? true : false,
                            message: "Direccion no encontrada en base de clientes",
                            boxPriceIsEqual: false
                        });
                        return;
                    }

                    console.log("Se encontro una coincidencia");
                    console.log("found", found['Comuna Despacho'], emailDate);
                    const deliveryDay = useRappiDeliverySchedule
                        ? findRappiDeliveryDayByComuna(
                            found['Comuna Despacho'],
                            emailDate,
                            getRegionFromClientRecord(found)
                        )
                        : findDeliveryDayByComuna(
                            found['Comuna Despacho'],
                            emailDate,
                            getRegionFromClientRecord(found)
                        );

                    if (deliveryDay != null) {
                        found['deliveryDay'] = `${deliveryDay}`;
                    } else {
                        found['deliveryDay'] = "";
                    }
                    resolve({
                        data: found,
                        length: [found].length,
                        address: true,
                        message: "Se encontro una coincidencia",
                        boxPriceIsEqual: boxPrice == found['Precio Caja'] ? true : false
                    });
                })
                .on('error', (error) => {
                    reject({ success: false, error: 'Error reading the CSV file', details: error });
                });
        } catch (error) {
            reject({ success: false, error: 'Error reading the CSV file', details: error });
        }
    });
}


export {
    readCSV,
    readEmailBody,
    readEmailBodyFromGmail,
    readManualOcExtractDate,
    readManualOcPreview,
    readManualOcDispatchPreview,
    readManualOcSubmit
};




