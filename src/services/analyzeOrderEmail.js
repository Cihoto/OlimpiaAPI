import { OpenAI } from 'openai';
import { GENERAL_ORDER_ANALYSIS_PROMPT,GMAIL_ORDER_ANALYSIS_PROMPT} from './prompts/orderExtractor.js';

const SYSTEM_INSTRUCTIONS = ''; // Instrucciones del sistema (puedes personalizarlas después)
const OLIMPIA_ASSISTANT_ID = process.env.OLIMPIA_ORDER_FINDER_ASSISTENT_ID; // ID del asistente en OpenAI Platform
const OPENAI_OLIMPIA_API_KEY = process.env.OPENAI_OLIMPIA; // Clave API de OpenAI



const ORDER_QUANTITY_KEYS = [
    'Pedido_Cantidad_Pink',
    'Pedido_Cantidad_Amargo',
    'Pedido_Cantidad_Leche',
    'Pedido_Cantidad_Free',
    'Pedido_Cantidad_Pink_90g',
    'Pedido_Cantidad_Amargo_90g',
    'Pedido_Cantidad_Leche_90g'
];

const EMPTY_ORDER_QUANTITIES = {
    Pedido_Cantidad_Pink: 0,
    Pedido_Cantidad_Amargo: 0,
    Pedido_Cantidad_Leche: 0,
    Pedido_Cantidad_Free: 0,
    Pedido_Cantidad_Pink_90g: 0,
    Pedido_Cantidad_Amargo_90g: 0,
    Pedido_Cantidad_Leche_90g: 0
};

const PEDIDOS_YA_PRODUCT_CODE_ENTRIES = [
    { code: 'Z69NYT', key: 'Pedido_Cantidad_Leche', unitsPerBox: 24 },
    { code: 'SAAKWF', key: 'Pedido_Cantidad_Free', unitsPerBox: 24 },
    { code: 'T4YJXL', key: 'Pedido_Cantidad_Amargo', unitsPerBox: 24 },
    { code: '7YZH72', key: 'Pedido_Cantidad_Pink', unitsPerBox: 24 },
    { code: '07798147783223', key: 'Pedido_Cantidad_Leche', unitsPerBox: 24 },
    { code: '7798147783223', key: 'Pedido_Cantidad_Leche', unitsPerBox: 24 },
    { code: '07798147784442', key: 'Pedido_Cantidad_Free', unitsPerBox: 24 },
    { code: '7798147784442', key: 'Pedido_Cantidad_Free', unitsPerBox: 24 },
    { code: '07798147780062', key: 'Pedido_Cantidad_Amargo', unitsPerBox: 24 },
    { code: '7798147780062', key: 'Pedido_Cantidad_Amargo', unitsPerBox: 24 },
    { code: '07798147784008', key: 'Pedido_Cantidad_Pink', unitsPerBox: 24 },
    { code: '7798147784008', key: 'Pedido_Cantidad_Pink', unitsPerBox: 24 }
];

const PEDIDOS_YA_PRODUCT_CODE_MAP = PEDIDOS_YA_PRODUCT_CODE_ENTRIES.reduce((acc, entry) => {
    acc[entry.code] = {
        key: entry.key,
        unitsPerBox: entry.unitsPerBox
    };
    return acc;
}, {});

const PEDIDOS_YA_PDF_SKU_CODES = Array.from(new Set(
    PEDIDOS_YA_PRODUCT_CODE_ENTRIES
        .map((entry) => normalizeProductCode(entry.code))
        .filter((code) => code.length === 6 && /[A-Z]/.test(code))
));

function normalizeText(value) {
    if (!value) {
        return '';
    }
    return String(value)
        .toLowerCase()
        .normalize('NFD')
        .replace(/\p{Diacritic}/gu, '')
        .replace(/[^a-z0-9]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function parseNumber(value) {
    if (value === null || value === undefined) {
        return 0;
    }
    const cleaned = String(value).replace(/[^0-9.-]/g, '');
    if (!cleaned) {
        return 0;
    }
    const num = Number(cleaned);
    return Number.isFinite(num) ? num : 0;
}

function normalizeProductCode(value) {
    return String(value || '')
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '')
        .trim();
}

function expandProductCodeCandidates(rawCode) {
    const normalized = normalizeProductCode(rawCode);
    if (!normalized) {
        return [];
    }
    const variants = [normalized];
    if (/^\d+$/.test(normalized)) {
        variants.push(normalized.replace(/^0+/, ''));
    }
    return Array.from(new Set(variants.filter(Boolean)));
}

function resolveProductMappingByCodes(codes = []) {
    for (const code of codes) {
        const candidates = expandProductCodeCandidates(code);
        for (const candidate of candidates) {
            if (PEDIDOS_YA_PRODUCT_CODE_MAP[candidate]) {
                return PEDIDOS_YA_PRODUCT_CODE_MAP[candidate];
            }
        }
    }
    return null;
}

const PRODUCT_GROUPS = [
    {
        key: 'Pedido_Cantidad_Free',
        patterns: [
            'free',
            'sin gluten',
            'sin azucar',
            's glu',
            's/glu',
            'chocolate free'
        ]
    },
    {
        key: 'Pedido_Cantidad_Pink',
        patterns: [
            'pink',
            'rosa',
            'framb cho pink',
            'cho pink',
            'choc pink',
            'bla pink',
            'blan pink',
            'blan pink',
            'blan pink'
        ]
    },
    {
        key: 'Pedido_Cantidad_Leche',
        patterns: [
            'leche',
            'lech',
            'leche y blanco',
            'blan lech',
            'blan lech',
            'blan lech',
            'choc blan lech',
            'cho blan lech',
            'lech bla',
            'lech bla',
            'choc blan lech',
            'franui dulce',
            'dulce'
        ]
    },
    {
        key: 'Pedido_Cantidad_Amargo',
        patterns: [
            'amargo',
            'negro',
            'neg',
            'neg blan',
            'blan neg',
            'neg blan',
            'choc neg',
            'cho neg',
            'franui negro'
        ]
    }
];

const NORMALIZED_PRODUCT_GROUPS = PRODUCT_GROUPS.map((group) => ({
    key: group.key,
    patterns: group.patterns.map((pattern) => normalizeText(pattern))
}));

function classifyProduct(description) {
    const normalized = normalizeText(description);
    if (!normalized) {
        return null;
    }

    const is90g = /\b90\s*(g|gr|gramos)\b/.test(normalized);

    for (const group of NORMALIZED_PRODUCT_GROUPS) {
        if (group.patterns.some((pattern) => pattern && normalized.includes(pattern))) {
            if (group.key === 'Pedido_Cantidad_Free') {
                return group.key;
            }
            if (is90g) {
                if (group.key === 'Pedido_Cantidad_Pink') return 'Pedido_Cantidad_Pink_90g';
                if (group.key === 'Pedido_Cantidad_Leche') return 'Pedido_Cantidad_Leche_90g';
                if (group.key === 'Pedido_Cantidad_Amargo') return 'Pedido_Cantidad_Amargo_90g';
            }
            return group.key;
        }
    }

    return null;
}

function normalizeHeader(value) {
    return normalizeText(value);
}

function normalizeOrderNumber(value) {
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

function parseDateToIso(value) {
    const raw = String(value || '').trim();
    if (!raw) {
        return null;
    }
    const ddmmyyyy = raw.match(/(\d{2})\/(\d{2})\/(\d{4})/);
    if (ddmmyyyy) {
        return `${ddmmyyyy[3]}-${ddmmyyyy[2]}-${ddmmyyyy[1]}`;
    }
    const yyyymmdd = raw.match(/(\d{4})-(\d{2})-(\d{2})/);
    if (yyyymmdd) {
        return `${yyyymmdd[1]}-${yyyymmdd[2]}-${yyyymmdd[3]}`;
    }
    return null;
}

function parsePedidosYaLegacyExcelText(text) {
    const quantities = { ...EMPTY_ORDER_QUANTITIES };
    let hasMatch = false;
    const lines = String(text || '')
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);

    let headerIndex = -1;
    let headerCols = [];
    for (let i = 0; i < lines.length; i += 1) {
        const normalizedLine = normalizeText(lines[i]);
        if (lines[i].includes('\t') && normalizedLine.includes('descripcion')) {
            headerIndex = i;
            headerCols = lines[i].split('\t');
            break;
        }
    }

    if (headerIndex === -1) {
        return null;
    }

    const normalizedCols = headerCols.map(normalizeHeader);
    const descIdx = normalizedCols.findIndex((col) => col.includes('descripcion'));
    const cajasIdx = normalizedCols.findIndex((col) => col.includes('cantidad') && col.includes('cajas'));
    const unidadesIdx = normalizedCols.findIndex((col) => col.includes('cantidad') && col.includes('unidades'));
    const skuIdx = normalizedCols.findIndex((col) => col === 'sku' || col.includes('sku'));
    const eanIdx = normalizedCols.findIndex((col) => col === 'ean' || col.includes('ean'));
    const internalCodeIdx = normalizedCols.findIndex((col) => col.includes('codigo') && col.includes('interno'));

    if (descIdx === -1 || (cajasIdx === -1 && unidadesIdx === -1)) {
        return null;
    }

    for (let i = headerIndex + 1; i < lines.length; i += 1) {
        const row = lines[i].split('\t');
        if (row.length <= descIdx) {
            continue;
        }

        const description = row[descIdx];
        const mappingByCode = resolveProductMappingByCodes([
            skuIdx !== -1 ? row[skuIdx] : null,
            eanIdx !== -1 ? row[eanIdx] : null,
            internalCodeIdx !== -1 ? row[internalCodeIdx] : null
        ]);
        const key = mappingByCode?.key || classifyProduct(description);
        if (!key) {
            continue;
        }

        const cajas = cajasIdx !== -1 && row.length > cajasIdx ? parseNumber(row[cajasIdx]) : 0;
        const unidades = unidadesIdx !== -1 && row.length > unidadesIdx ? parseNumber(row[unidadesIdx]) : 0;

        let boxes = cajas;
        if (!boxes && unidades) {
            const boxSize = mappingByCode?.unitsPerBox || (key.includes('_90g') ? 18 : 24);
            if (unidades % boxSize === 0) {
                boxes = unidades / boxSize;
            }
        }

        if (boxes) {
            quantities[key] += boxes;
            hasMatch = true;
        }
    }

    return hasMatch ? quantities : null;
}

function parseBoxesFromUnitsChunk(unitsChunk, unitsPerBox) {
    const digits = String(unitsChunk || '').replace(/\D/g, '');
    if (!digits) {
        return 0;
    }

    const parsedUnitsPerBox = Number(unitsPerBox);
    if (Number.isFinite(parsedUnitsPerBox) && parsedUnitsPerBox > 0 && digits.length >= 2) {
        const maxBoxDigits = Math.min(3, digits.length - 1);
        for (let boxDigits = 1; boxDigits <= maxBoxDigits; boxDigits += 1) {
            const boxes = Number(digits.slice(0, boxDigits));
            const units = Number(digits.slice(boxDigits));
            if (!Number.isFinite(boxes) || !Number.isFinite(units) || boxes <= 0 || units <= 0) {
                continue;
            }
            if (units % parsedUnitsPerBox === 0 && (units / parsedUnitsPerBox) === boxes) {
                return boxes;
            }
        }
    }

    if (digits.length <= 2) {
        const boxes = parseNumber(digits);
        return boxes > 0 ? boxes : 0;
    }

    return 0;
}

function parsePedidosYaLegacyPdfText(text) {
    const quantities = { ...EMPTY_ORDER_QUANTITIES };
    let hasMatch = false;
    const lines = String(text || '')
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);

    for (const rawLine of lines) {
        const compactLine = rawLine.replace(/\s+/g, '');
        const rowMatch = compactLine.match(/^([A-Z0-9]{6})(\d{12,14})(.+)$/);
        if (!rowMatch) {
            continue;
        }

        const sku = rowMatch[1];
        const ean = rowMatch[2];
        const rowTail = rowMatch[3];

        const mappingByCode = resolveProductMappingByCodes([sku, ean]);
        if (!mappingByCode) {
            continue;
        }

        const unitsChunkMatch = rowTail.match(/(\d{2,7})(?=\d{1,3}(?:[.,]\d{3})+[.,]\d{2}\$?)/);
        if (!unitsChunkMatch) {
            continue;
        }

        const boxes = parseBoxesFromUnitsChunk(unitsChunkMatch[1], mappingByCode.unitsPerBox);
        if (!boxes) {
            continue;
        }

        quantities[mappingByCode.key] += boxes;
        hasMatch = true;
    }

    return hasMatch ? quantities : null;
}

function parsePedidosYaSkuExportOrdersFromText(text) {
    const lines = String(text || '')
        .split(/\r?\n/)
        .map((line) => String(line || '').trim())
        .filter(Boolean);

    let headerIndex = -1;
    let headerCols = [];
    for (let i = 0; i < lines.length; i += 1) {
        if (!lines[i].includes('\t')) {
            continue;
        }
        const normalizedLine = normalizeText(lines[i]);
        if (normalizedLine.includes('po number') && normalizedLine.includes('supplier sku')) {
            headerIndex = i;
            headerCols = lines[i].split('\t');
            break;
        }
    }

    if (headerIndex === -1) {
        return [];
    }

    const cols = headerCols.map(normalizeHeader);
    const poIdx = cols.findIndex((col) => col === 'po number' || col.includes('po number'));
    const poCreationIdx = cols.findIndex((col) => col.includes('po creation date'));
    const poReceivingIdx = cols.findIndex((col) => col.includes('po receiving date') || col.includes('po expected delivery at'));
    const skuIdx = cols.findIndex((col) => col.includes('supplier sku'));
    const barcodeIdx = cols.findIndex((col) => col === 'barcode' || col.includes('barcode'));
    const productNameIdx = cols.findIndex((col) => col.includes('product name'));
    const unitsPerCaseIdx = cols.findIndex((col) => col.includes('units per case'));
    const totalCaseIdx = cols.findIndex((col) => col.includes('total ordered case'));
    const orderedQtyIdx = cols.findIndex((col) => col.includes('ordered qty'));
    const storeNameIdx = cols.findIndex((col) => col.includes('store name'));
    const storeAddressIdx = cols.findIndex((col) => col.includes('store address'));

    if (poIdx === -1) {
        return [];
    }

    const groupedOrders = new Map();
    for (let i = headerIndex + 1; i < lines.length; i += 1) {
        if (!lines[i].includes('\t')) {
            continue;
        }
        const row = lines[i].split('\t');
        const orderNumber = normalizeOrderNumber(row[poIdx]);
        if (!orderNumber) {
            continue;
        }

        if (!groupedOrders.has(orderNumber)) {
            groupedOrders.set(orderNumber, {
                orderNumber,
                orderDate: null,
                deliveryDate: null,
                storeName: '',
                storeAddress: '',
                quantities: { ...EMPTY_ORDER_QUANTITIES },
                itemCount: 0,
                rows: []
            });
        }

        const current = groupedOrders.get(orderNumber);
        const rowOrderDate = parseDateToIso(poCreationIdx !== -1 ? row[poCreationIdx] : null);
        const rowDeliveryDate = parseDateToIso(poReceivingIdx !== -1 ? row[poReceivingIdx] : null);
        const rowStoreName = String(storeNameIdx !== -1 ? row[storeNameIdx] : '').trim();
        const rowStoreAddress = String(storeAddressIdx !== -1 ? row[storeAddressIdx] : '').trim();
        if (!current.orderDate && rowOrderDate) current.orderDate = rowOrderDate;
        if (!current.deliveryDate && rowDeliveryDate) current.deliveryDate = rowDeliveryDate;
        if (!current.storeName && rowStoreName) current.storeName = rowStoreName;
        if (!current.storeAddress && rowStoreAddress) current.storeAddress = rowStoreAddress;

        const productName = String(productNameIdx !== -1 ? row[productNameIdx] : '').trim();
        const mappingByCode = resolveProductMappingByCodes([
            skuIdx !== -1 ? row[skuIdx] : null,
            barcodeIdx !== -1 ? row[barcodeIdx] : null
        ]);
        const quantityKey = mappingByCode?.key || classifyProduct(productName);

        const parsedTotalCase = totalCaseIdx !== -1 ? parseNumber(row[totalCaseIdx]) : 0;
        const parsedOrderedQty = orderedQtyIdx !== -1 ? parseNumber(row[orderedQtyIdx]) : 0;
        const parsedUnitsPerCase = unitsPerCaseIdx !== -1 ? parseNumber(row[unitsPerCaseIdx]) : 0;
        const unitsPerCase = parsedUnitsPerCase || mappingByCode?.unitsPerBox || (quantityKey?.includes('_90g') ? 18 : 24);

        let boxes = parsedTotalCase;
        if (!boxes && parsedOrderedQty && unitsPerCase && parsedOrderedQty % unitsPerCase === 0) {
            boxes = parsedOrderedQty / unitsPerCase;
        }

        const rowRecord = {
            sku: skuIdx !== -1 ? String(row[skuIdx] || '').trim() : '',
            barcode: barcodeIdx !== -1 ? String(row[barcodeIdx] || '').trim() : '',
            productName,
            totalOrderedCase: boxes,
            orderedQty: parsedOrderedQty,
            unitsPerCase
        };
        current.rows.push(rowRecord);

        if (quantityKey && boxes > 0) {
            current.quantities[quantityKey] += boxes;
            current.itemCount += 1;
        }
    }

    return Array.from(groupedOrders.values())
        .sort((a, b) => String(a.orderNumber).localeCompare(String(b.orderNumber)))
        .map((order) => {
            const header = [
                'po_number',
                'po_creation_date',
                'po_expected_delivery_at',
                'supplier_sku',
                'barcode',
                'product_name',
                'units_per_case',
                'total_ordered_case',
                'ordered_qty',
                'store_name',
                'store_address'
            ].join('\t');
            const rowsText = order.rows.map((row) => ([
                order.orderNumber,
                order.orderDate || '',
                order.deliveryDate || '',
                row.sku,
                row.barcode,
                row.productName,
                row.unitsPerCase || '',
                row.totalOrderedCase || '',
                row.orderedQty || '',
                order.storeName || '',
                order.storeAddress || ''
            ].join('\t'))).join('\n');

            return {
                orderNumber: order.orderNumber,
                orderDate: order.orderDate || null,
                deliveryDate: order.deliveryDate || null,
                storeName: order.storeName || null,
                storeAddress: order.storeAddress || null,
                quantities: order.quantities,
                itemCount: order.itemCount || 0,
                sourceFormat: 'peya_excel_sku_export_v2',
                orderText: `Sheet: SKU-REPORT\n${header}\n${rowsText}`.trim()
            };
        });
}

function decodePdfBoxesFromEaDigits(digits, unitsPerBoxHint) {
    const cleanDigits = String(digits || '').replace(/\D/g, '');
    if (!cleanDigits) {
        return 0;
    }

    const hint = Number(unitsPerBoxHint);
    let bestCandidate = null;

    for (let unitsLength = 1; unitsLength <= 2; unitsLength += 1) {
        const unitsPerCase = Number(cleanDigits.slice(0, unitsLength));
        if (!Number.isFinite(unitsPerCase) || unitsPerCase <= 0 || unitsPerCase > 200) {
            continue;
        }

        for (let caseLength = 1; caseLength <= 3; caseLength += 1) {
            const caseStart = unitsLength;
            const caseEnd = caseStart + caseLength;
            if (caseEnd >= cleanDigits.length) {
                continue;
            }
            const caseQty = Number(cleanDigits.slice(caseStart, caseEnd));
            if (!Number.isFinite(caseQty) || caseQty <= 0) {
                continue;
            }

            for (let orderedLength = 1; orderedLength <= 4; orderedLength += 1) {
                const orderedStart = caseEnd;
                const orderedEnd = orderedStart + orderedLength;
                if (orderedEnd >= cleanDigits.length) {
                    continue;
                }
                const orderedQty = Number(cleanDigits.slice(orderedStart, orderedEnd));
                if (!Number.isFinite(orderedQty) || orderedQty <= 0) {
                    continue;
                }
                if (orderedQty !== caseQty * unitsPerCase) {
                    continue;
                }

                for (let unitCostLength = 3; unitCostLength <= 5; unitCostLength += 1) {
                    const unitStart = orderedEnd;
                    const unitEnd = unitStart + unitCostLength;
                    if (unitEnd >= cleanDigits.length) {
                        continue;
                    }
                    const unitCost = Number(cleanDigits.slice(unitStart, unitEnd));
                    if (!Number.isFinite(unitCost) || unitCost <= 0) {
                        continue;
                    }

                    for (let discountedLength = 3; discountedLength <= 5; discountedLength += 1) {
                        const discountedStart = unitEnd;
                        const discountedEnd = discountedStart + discountedLength;
                        if (discountedEnd >= cleanDigits.length) {
                            continue;
                        }
                        const discountedUnitCost = Number(cleanDigits.slice(discountedStart, discountedEnd));
                        if (!Number.isFinite(discountedUnitCost) || discountedUnitCost <= 0) {
                            continue;
                        }

                        const amountDigits = cleanDigits.slice(discountedEnd);
                        if (!amountDigits || amountDigits.length < 4) {
                            continue;
                        }
                        const amountExclTax = Number(amountDigits);
                        if (!Number.isFinite(amountExclTax) || amountExclTax <= 0) {
                            continue;
                        }

                        const expectedAmount = orderedQty * discountedUnitCost;
                        const amountDiff = Math.abs(amountExclTax - expectedAmount);
                        const unitsPenalty = Number.isFinite(hint) && hint > 0 && unitsPerCase !== hint ? 1000000 : 0;
                        const score = amountDiff + unitsPenalty;

                        if (!bestCandidate || score < bestCandidate.score) {
                            bestCandidate = {
                                score,
                                caseQty
                            };
                        }
                    }
                }
            }
        }
    }

    return bestCandidate?.caseQty || 0;
}

function parsePedidosYaPdfOrdersFromText(text) {
    const rawText = String(text || '');
    if (!rawText.trim()) {
        return [];
    }

    const orderNumber = normalizeOrderNumber(extractOrderNumberFromText(rawText));
    if (!orderNumber) {
        return [];
    }

    const compactText = rawText.replace(/\s+/g, '');
    const orderDateMatch = compactText.match(new RegExp(`${orderNumber}(\\d{2}\\/\\d{2}\\/\\d{4})\\d{1,2}:\\d{2}(\\d{2}\\/\\d{2}\\/\\d{4})`));
    const orderDate = parseDateToIso(orderDateMatch?.[1] || null);
    const deliveryDate = parseDateToIso(orderDateMatch?.[2] || null);

    const lines = rawText
        .split(/\r?\n/)
        .map((line) => String(line || '').trim())
        .filter(Boolean);
    let storeName = null;
    let storeAddress = null;
    const storeInfoIndex = lines.findIndex((line) => normalizeText(line) === 'store information');
    if (storeInfoIndex !== -1) {
        const nextLine = String(lines[storeInfoIndex + 1] || '').trim();
        if (nextLine) {
            storeName = nextLine;
        }
        const addressParts = [];
        for (let i = storeInfoIndex + 2; i < lines.length; i += 1) {
            const normalized = normalizeText(lines[i]);
            if (normalized.includes('order number') && normalized.includes('order date')) {
                break;
            }
            addressParts.push(String(lines[i] || '').trim());
        }
        storeAddress = addressParts.join(' ').trim() || null;
    }

    const quantities = { ...EMPTY_ORDER_QUANTITIES };
    let itemCount = 0;
    const skuPattern = PEDIDOS_YA_PDF_SKU_CODES.join('|');
    if (!skuPattern) {
        return [];
    }

    const rowRegex = new RegExp(
        `\\d(${skuPattern})(\\d{12,14})([\\s\\S]*?)(?=\\d(?:${skuPattern})\\d{12,14}|TotalAmt(?:\\\\.Excl\\\\.Tax|ExclTax):|$)`,
        'g'
    );
    const seenRows = new Set();
    let rowMatch;
    while ((rowMatch = rowRegex.exec(compactText)) !== null) {
        const sku = rowMatch[1];
        const barcode = rowMatch[2];
        const tail = rowMatch[3];
        const rowKey = `${sku}:${barcode}:${tail.slice(0, 40)}`;
        if (seenRows.has(rowKey)) {
            continue;
        }
        seenRows.add(rowKey);

        const mappingByCode = resolveProductMappingByCodes([sku, barcode]);
        if (!mappingByCode) {
            continue;
        }

        const eaMatch = tail.match(/EA(\d+)0%19(?:[.,]00)?%/);
        if (!eaMatch) {
            continue;
        }

        const boxes = decodePdfBoxesFromEaDigits(eaMatch[1], mappingByCode.unitsPerBox);
        if (!boxes) {
            continue;
        }

        quantities[mappingByCode.key] += boxes;
        itemCount += 1;
    }

    return [{
        orderNumber,
        orderDate: orderDate || null,
        deliveryDate: deliveryDate || null,
        storeName,
        storeAddress,
        quantities,
        itemCount,
        sourceFormat: 'peya_pdf_purchase_order_v2',
        orderText: rawText
    }];
}

function detectPedidosYaAttachmentType(payload) {
    const attachmentFilename = String(payload?.attachmentFilename || '').toLowerCase();
    const mimeType = String(payload?.mimeType || payload?.attachmentMimeType || '').toLowerCase();
    const attachedText = String(payload?.emailAttached || '');
    const normalizedSnippet = normalizeText(attachedText.slice(0, 2500));

    if (attachmentFilename.endsWith('.pdf') || mimeType.includes('pdf')) {
        return 'pdf';
    }

    if (/\.xlsx?$/i.test(attachmentFilename) || mimeType.includes('spreadsheetml') || mimeType.includes('ms-excel')) {
        return 'excel';
    }

    if (attachedText.includes('\t') && normalizedSnippet.includes('descripcion')) {
        return 'excel';
    }

    if (normalizedSnippet.includes('n sku') && normalizedSnippet.includes('cantidad cajas')) {
        return 'pdf';
    }

    return 'unknown';
}

function extractPedidosYaOrdersFromAttachment(emailContent) {
    try {
        const payload = typeof emailContent === 'string' ? JSON.parse(emailContent) : emailContent;
        if (!payload || typeof payload !== 'object') {
            return [];
        }

        const emailAttached = payload.emailAttached;
        if (!emailAttached || typeof emailAttached !== 'string') {
            return [];
        }

        const attachmentType = detectPedidosYaAttachmentType(payload);
        if (attachmentType === 'excel') {
            const orders = parsePedidosYaSkuExportOrdersFromText(emailAttached);
            if (orders.length > 0) {
                return orders;
            }
        }
        if (attachmentType === 'pdf') {
            const orders = parsePedidosYaPdfOrdersFromText(emailAttached);
            if (orders.length > 0) {
                return orders;
            }
        }

        const fallbackOrderNumber = extractOrderNumberFromText(payload.attachmentFilename)
            || extractOrderNumberFromText(emailAttached)
            || extractOrderNumberFromText(payload.emailSubject)
            || extractOrderNumberFromText(payload.emailBody);
        const fallbackQuantities = parsePedidosYaLegacyExcelText(emailAttached) || parsePedidosYaLegacyPdfText(emailAttached);
        if (!fallbackOrderNumber && !fallbackQuantities) {
            return [];
        }
        return [{
            orderNumber: fallbackOrderNumber || null,
            orderDate: null,
            deliveryDate: null,
            storeName: null,
            storeAddress: null,
            quantities: fallbackQuantities || { ...EMPTY_ORDER_QUANTITIES },
            itemCount: 0,
            sourceFormat: 'peya_legacy_fallback',
            orderText: String(emailAttached || '')
        }];
    } catch (error) {
        return [];
    }
}

function parsePedidosYaOrderQuantities(emailContent) {
    try {
        const payload = typeof emailContent === 'string' ? JSON.parse(emailContent) : emailContent;
        if (!payload || typeof payload !== 'object') {
            return null;
        }
        const targetOrderNumber = normalizeOrderNumber(
            payload?.manualOc?.targetOrderNumber || payload?.targetOrderNumber
        );
        const orders = extractPedidosYaOrdersFromAttachment(payload);
        if (orders.length > 0) {
            const selectedOrder = targetOrderNumber
                ? orders.find((order) => normalizeOrderNumber(order.orderNumber) === targetOrderNumber) || null
                : orders[0];
            return selectedOrder?.quantities || null;
        }
        const emailAttached = payload.emailAttached;
        if (!emailAttached || typeof emailAttached !== 'string') {
            return null;
        }
        return parsePedidosYaLegacyExcelText(emailAttached) || parsePedidosYaLegacyPdfText(emailAttached);
    } catch (error) {
        return null;
    }
}

function extractOrderNumberFromText(value) {
    if (!value) {
        return null;
    }

    const match = String(value).toUpperCase().match(/\bPO\s*[-_]?(\d{5,})\b/);
    if (!match) {
        return null;
    }

    return `PO${match[1]}`;
}

function extractPedidosYaOrderNumber(emailContent) {
    try {
        const payload = typeof emailContent === 'string' ? JSON.parse(emailContent) : emailContent;
        if (!payload || typeof payload !== 'object') {
            return null;
        }
        const targetOrderNumber = normalizeOrderNumber(
            payload?.manualOc?.targetOrderNumber || payload?.targetOrderNumber
        );
        const parsedOrders = extractPedidosYaOrdersFromAttachment(payload);
        if (parsedOrders.length > 0) {
            if (targetOrderNumber) {
                const targetOrder = parsedOrders.find(
                    (order) => normalizeOrderNumber(order.orderNumber) === targetOrderNumber
                );
                if (targetOrder?.orderNumber) {
                    return normalizeOrderNumber(targetOrder.orderNumber);
                }
            }
            if (parsedOrders[0]?.orderNumber) {
                return normalizeOrderNumber(parsedOrders[0].orderNumber);
            }
        }

        const candidates = [
            payload.attachmentFilename,
            payload.emailAttached,
            payload.emailSubject,
            payload.emailBody
        ];

        for (const candidate of candidates) {
            const extracted = extractOrderNumberFromText(candidate);
            if (extracted) {
                return extracted;
            }
        }

        return null;
    } catch (error) {
        return null;
    }
}


let openaiClient = null;
function getOpenAIClient() {
    if (!openaiClient) {
        openaiClient = new OpenAI({ apiKey: OPENAI_OLIMPIA_API_KEY });
    }
    return openaiClient;
}
async function analyzeOrderEmail(emailContent) {
    try {
        const openai = getOpenAIClient();
        const response = await openai.responses.create({
            model: 'gpt-4o-mini',
            input: [
                { role: 'system', content: GENERAL_ORDER_ANALYSIS_PROMPT },
                { role: 'user', content: emailContent }
            ],
            text: {
                format: {
                    type: 'json_schema',
                    name: 'order_quantities',
                    schema: {
                        type: 'object',
                        additionalProperties: false,
                        required: [
                            'Pedido_Cantidad_Pink',
                            'Pedido_Cantidad_Amargo',
                            'Pedido_Cantidad_Leche',
                            'Pedido_Cantidad_Free',
                            'Pedido_Cantidad_Pink_90g',
                            'Pedido_Cantidad_Amargo_90g',
                            'Pedido_Cantidad_Leche_90g'
                        ],
                        properties: {
                            Pedido_Cantidad_Pink: { type: 'number' },
                            Pedido_Cantidad_Amargo: { type: 'number' },
                            Pedido_Cantidad_Leche: { type: 'number' },
                            Pedido_Cantidad_Free: { type: 'number' },
                            Pedido_Cantidad_Pink_90g: { type: 'number' },
                            Pedido_Cantidad_Amargo_90g: { type: 'number' },
                            Pedido_Cantidad_Leche_90g: { type: 'number' }
                        }
                    },
                    strict: true
                }
            },
            temperature: 0
        });

        const outputText = response.output_text || '';
        const parsed = parseOrderQuantitiesJson(outputText);
        if (!parsed) {
            return { ...EMPTY_ORDER_QUANTITIES };
        }
        return parsed;
    } catch (error) {
        console.error('Error analyzing order email:', error);
        return { ...EMPTY_ORDER_QUANTITIES };
    }
}

async function analyzeOrderEmailFromGmail(emailContent) {
    try {
        const deterministic = parsePedidosYaOrderQuantities(emailContent);
        if (deterministic) {
            return deterministic;
        }

        const openai = getOpenAIClient();
        const response = await openai.chat.completions.create({
            model: 'gpt-4o-mini',
            messages: [
                { role: 'system', content: GMAIL_ORDER_ANALYSIS_PROMPT },
                { role: 'user', content: `Contenido a analizar (JSON): ${emailContent}` }
            ],
            temperature: 0
        });

        const jsonResponse = response.choices?.[0]?.message?.content?.trim() || '';
        const parsed = parseOrderQuantitiesJson(jsonResponse);
        if (!parsed) {
            return { ...EMPTY_ORDER_QUANTITIES };
        }
        return parsed;
    } catch (error) {
        console.error('Error analyzing order email from gmail:', error);
        return { ...EMPTY_ORDER_QUANTITIES };
    }
}

function parseToJson(reply) {
    try {
        // Intenta parsear el reply a JSON
        const parsed = JSON.parse(reply);
        return parsed;
    } catch (error) {
        // Si no es posible parsear, devuelve el reply sin cambios
        console.warn("⚠️ No se pudo parsear el reply a JSON:", error.message);
        return reply;
    }
}

function parseOrderQuantitiesJson(reply) {
    try {
        const sanitizedOutput = reply
            .replace(/```json|```/g, '')
            .replace(/\r?\n/g, '')
            .replace(/\\/g, '')
            .trim();
        const parsed = JSON.parse(sanitizedOutput);
        const data = Array.isArray(parsed) ? parsed[0] : parsed;
        if (!data || typeof data !== 'object') {
            return null;
        }
        return normalizeOrderQuantities(data);
    } catch (error) {
        console.warn('No se pudo parsear el reply a JSON:', error.message);
        return null;
    }
}

function normalizeOrderQuantities(data) {
    const output = {};
    ORDER_QUANTITY_KEYS.forEach((key) => {
        const value = Object.prototype.hasOwnProperty.call(data, key) ? data[key] : 0;
        if (typeof value === 'number' && Number.isFinite(value)) {
            output[key] = value;
            return;
        }
        if (typeof value === 'string') {
            const cleaned = value.replace(/[^0-9.-]/g, '');
            const num = cleaned === '' ? NaN : Number(cleaned);
            output[key] = Number.isFinite(num) ? num : 0;
            return;
        }
        const num = Number(value);
        output[key] = Number.isFinite(num) ? num : 0;
    });
    return output;
}

export {
    analyzeOrderEmail,
    analyzeOrderEmailFromGmail,
    parsePedidosYaOrderQuantities,
    extractPedidosYaOrderNumber,
    extractPedidosYaOrdersFromAttachment
};
