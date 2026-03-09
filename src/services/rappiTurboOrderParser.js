import { EMPTY_ORDER_QUANTITIES } from './keyLogisticsOrderParser.js';

const BOX_SIZE = 24;

const RAPPI_CODE_MAP = Object.freeze({
    '7798147780055': {
        quantityKey: 'Pedido_Cantidad_Leche',
        totalKey: 'Pedido_PrecioTotal_Leche'
    },
    '7798147780062': {
        quantityKey: 'Pedido_Cantidad_Amargo',
        totalKey: 'Pedido_PrecioTotal_Amargo'
    },
    '7798147784008': {
        quantityKey: 'Pedido_Cantidad_Pink',
        totalKey: 'Pedido_PrecioTotal_Pink'
    },
    '7798147784442': {
        quantityKey: 'Pedido_Cantidad_Free',
        totalKey: 'Pedido_PrecioTotal_Free'
    }
});

const BLOCKED_RUTS = new Set(['77419327-8']);
const EMPTY_PRICE_TOTALS = Object.freeze({
    Pedido_PrecioTotal_Pink: 0,
    Pedido_PrecioTotal_Amargo: 0,
    Pedido_PrecioTotal_Leche: 0,
    Pedido_PrecioTotal_Free: 0,
    Pedido_PrecioTotal_Pink_90g: 0,
    Pedido_PrecioTotal_Amargo_90g: 0,
    Pedido_PrecioTotal_Leche_90g: 0
});

function normalizeSpaces(value) {
    return String(value || '').replace(/\s+/g, ' ').trim();
}

function normalizeRutValue(value) {
    const clean = String(value || '').replace(/[^0-9kK]/g, '').toUpperCase();
    if (clean.length < 2) {
        return '';
    }
    const body = clean.slice(0, -1);
    const verifier = clean.slice(-1);
    return `${body}-${verifier}`;
}

function extractRuts(text) {
    const matches = String(text || '').match(/\b(?:\d{1,2}(?:\.\d{3}){1,2}|\d{7,8})-[0-9kK]\b/g) || [];
    const result = [];
    for (const match of matches) {
        const normalized = normalizeRutValue(match);
        if (normalized) {
            result.push(normalized);
        }
    }
    return Array.from(new Set(result));
}

function parseMoney(value) {
    const raw = String(value || '').replace(/[^\d.,-]/g, '').trim();
    if (!raw) {
        return null;
    }

    const hasComma = raw.includes(',');
    const hasDot = raw.includes('.');
    let normalized = raw;

    if (hasComma && hasDot) {
        normalized = raw.replace(/\./g, '').replace(',', '.');
    } else if (hasComma) {
        normalized = raw.replace(',', '.');
    }

    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : null;
}

function normalizeIntegerMoney(value) {
    if (!Number.isFinite(value)) {
        return 0;
    }
    return Math.round(value);
}

function extractOcNumber(text) {
    const content = String(text || '');
    const patterns = [
        /num\.?\s*oc\s*:\s*(\d{6,})/i,
        /orden de compra[^0-9]{0,30}(\d{6,})/i,
        /\b(45\d{8})\b/,
        /\b(18\d{4,})\b/
    ];

    for (const pattern of patterns) {
        const match = content.match(pattern);
        if (match) {
            return match[1];
        }
    }

    return null;
}

function extractStoreName(text) {
    const content = String(text || '');
    const match = content.match(/tienda:\s*([^\n]+?)(?:\s+direccion:|$)/i);
    return match ? normalizeSpaces(match[1]) : null;
}

function extractDispatchAddress(text) {
    const content = String(text || '');
    const inlineMatch = content.match(/direccion:\s*([\s\S]*?)\s+despacho:/i);
    if (inlineMatch) {
        return normalizeSpaces(inlineMatch[1]);
    }

    const lines = content.split(/\r?\n/);
    for (const line of lines) {
        const match = line.match(/direccion:\s*(.+)$/i);
        if (match) {
            return normalizeSpaces(match[1]);
        }
    }

    return null;
}

function extractTotals(text) {
    const content = String(text || '');
    const match = content.match(/subtotal:\s*([0-9.,]+)\s*iva:\s*([0-9.,]+)\s*total:\s*([0-9.,]+)/i);
    if (!match) {
        return null;
    }

    return {
        subtotal: parseMoney(match[1]),
        iva: parseMoney(match[2]),
        total: parseMoney(match[3]),
        subtotalRaw: match[1],
        ivaRaw: match[2],
        totalRaw: match[3]
    };
}

function extractProductLines(text) {
    const compact = String(text || '').replace(/\r/g, '').replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
    const items = [];
    const productRegex = /(\d{13})-([\s\S]*?)(?=\d{13}-|Subtotal:|$)/gi;
    let match;

    while ((match = productRegex.exec(compact)) !== null) {
        const code = match[1];
        const segment = normalizeSpaces(match[2]);
        const tailMatch = segment.match(
            /(\d{1,4})\s*([0-9.]{4,5},\d{1,2})\s*([0-9.]{4,8},\d{1,2})$/i
        );

        let quantity = null;
        let unitPrice = null;
        let lineTotal = null;
        let description = segment;

        if (tailMatch) {
            quantity = Number.parseInt(tailMatch[1], 10);
            unitPrice = parseMoney(tailMatch[2]);
            lineTotal = parseMoney(tailMatch[3]);
            const tailIndex = segment.lastIndexOf(tailMatch[0]);
            description = normalizeSpaces(segment.slice(0, tailIndex));
        }

        items.push({
            code,
            description,
            quantity: Number.isFinite(quantity) ? quantity : null,
            unitPrice,
            lineTotal,
            raw: segment
        });
    }

    return items;
}

function convertToBoxes(rawQuantity) {
    if (!Number.isFinite(rawQuantity) || rawQuantity <= 0) {
        return 0;
    }

    if (rawQuantity % BOX_SIZE === 0) {
        return rawQuantity / BOX_SIZE;
    }

    // Some customers send "1" meaning one box, even if they do not specify units.
    return rawQuantity;
}

function parseRappiTurboOrderText(text) {
    const safeText = String(text || '');
    const quantities = { ...EMPTY_ORDER_QUANTITIES };
    const priceTotals = { ...EMPTY_PRICE_TOTALS };
    const products = extractProductLines(safeText);
    const unknownCodes = [];

    for (const product of products) {
        const mapping = RAPPI_CODE_MAP[product.code];
        if (!mapping) {
            unknownCodes.push(product.code);
            continue;
        }

        const boxes = convertToBoxes(product.quantity || 0);
        if (boxes > 0) {
            quantities[mapping.quantityKey] += boxes;
        }
        if (Number.isFinite(product.lineTotal)) {
            priceTotals[mapping.totalKey] += normalizeIntegerMoney(product.lineTotal);
        }
    }

    const ruts = extractRuts(safeText);
    const rut = ruts.find((candidate) => !BLOCKED_RUTS.has(candidate)) || null;
    const dispatchAddress = extractDispatchAddress(safeText);
    const storeName = extractStoreName(safeText);

    const direccionesEncontradas = [];
    if (dispatchAddress) {
        direccionesEncontradas.push(dispatchAddress);
    }

    return {
        ocNumber: extractOcNumber(safeText),
        rut,
        storeName,
        dispatchAddress,
        direccionesEncontradas,
        totals: extractTotals(safeText),
        priceTotals,
        quantities,
        products,
        unknownCodes: Array.from(new Set(unknownCodes))
    };
}

export { parseRappiTurboOrderText, RAPPI_CODE_MAP };
