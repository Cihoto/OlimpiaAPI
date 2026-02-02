const ORDER_QUANTITY_KEYS = [
    'Pedido_Cantidad_Pink',
    'Pedido_Cantidad_Amargo',
    'Pedido_Cantidad_Leche',
    'Pedido_Cantidad_Free',
    'Pedido_Cantidad_Pink_90g',
    'Pedido_Cantidad_Amargo_90g',
    'Pedido_Cantidad_Leche_90g'
];

const EMPTY_ORDER_QUANTITIES = ORDER_QUANTITY_KEYS.reduce((acc, key) => {
    acc[key] = 0;
    return acc;
}, {});

const BLOCKED_RUTS = new Set([
    '77419327-8', // Olimpia SPA
    '96930440-6'  // Key Logistics
]);

const KEY_LOGISTICS_CLIENTS = [
    {
        id: 'esmax',
        aliases: [
            'esmax red ltda',
            'esmax red ltda owner',
            'esmax red'
        ],
        codeMap: {
            '100912427': { key: 'Pedido_Cantidad_Amargo', boxSize: 24 },
            '219838': { key: 'Pedido_Cantidad_Amargo', boxSize: 24 },
            '100912428': { key: 'Pedido_Cantidad_Leche', boxSize: 24 },
            '219837': { key: 'Pedido_Cantidad_Leche', boxSize: 24 },
            '100915324': { key: 'Pedido_Cantidad_Pink', boxSize: 24 },
            '100916142': { key: 'Pedido_Cantidad_Free', boxSize: 24 }
        }
    },
    {
        id: 'adm_ventas',
        aliases: [
            'adm de ventas al detalle ltda',
            'adm de ventas al detalle'
        ],
        codeMap: {
            '100912427': { key: 'Pedido_Cantidad_Amargo', boxSize: 24 },
            '100912428': { key: 'Pedido_Cantidad_Leche', boxSize: 24 },
            '100915377': { key: 'Pedido_Cantidad_Pink', boxSize: 24 }
        }
    },
    {
        id: 'enex',
        aliases: [
            'emp nac de energia enex',
            'emp nac de energia enex s a',
            'enex s a',
            'enex'
        ],
        codeMap: {
            '100912586': { key: 'Pedido_Cantidad_Amargo', boxSize: 24 },
            '100912587': { key: 'Pedido_Cantidad_Leche', boxSize: 24 },
            '100914872': { key: 'Pedido_Cantidad_Pink', boxSize: 24 }
        }
    },
    {
        id: 'oxxo',
        aliases: [
            'cadena comercial oxxo chile s a',
            'cadena comercial oxxo chile',
            'oxxo'
        ],
        codeMap: {
            '100913772': { key: 'Pedido_Cantidad_Amargo', boxSize: 24 },
            '100913773': { key: 'Pedido_Cantidad_Leche', boxSize: 24 },
            '100914830': { key: 'Pedido_Cantidad_Pink', boxSize: 24 },
            '100916189': { key: 'Pedido_Cantidad_Free', boxSize: 24 }
        }
    }
];

const CODE_TO_CLIENT = new Map();
for (const client of KEY_LOGISTICS_CLIENTS) {
    for (const [code, mapping] of Object.entries(client.codeMap)) {
        if (!CODE_TO_CLIENT.has(code)) {
            CODE_TO_CLIENT.set(code, []);
        }
        CODE_TO_CLIENT.get(code).push({ clientId: client.id, mapping });
    }
}

function normalizeText(value) {
    if (!value) {
        return '';
    }
    return value
        .toLowerCase()
        .normalize('NFD')
        .replace(/\p{Diacritic}/gu, '')
        .replace(/[^a-z0-9]+/g, ' ')
        .trim();
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
    const matches = text.match(/\b(\d{1,2}(?:\.\d{3}){1,2}|\d{7,8})-[0-9kK]\b/g) || [];
    return matches.map((match) => normalizeRutValue(match)).filter(Boolean);
}

function findClientByAlias(normalizedText) {
    if (!normalizedText) {
        return null;
    }
    return KEY_LOGISTICS_CLIENTS.find((client) =>
        client.aliases.some((alias) => normalizedText.includes(alias))
    );
}

function findClientByCodes(codes) {
    const counts = new Map();
    codes.forEach((code) => {
        const entries = CODE_TO_CLIENT.get(code) || [];
        entries.forEach((entry) => {
            counts.set(entry.clientId, (counts.get(entry.clientId) || 0) + 1);
        });
    });

    let bestClient = null;
    let bestCount = 0;
    for (const [clientId, count] of counts.entries()) {
        if (count > bestCount) {
            bestClient = clientId;
            bestCount = count;
        }
    }

    if (!bestClient) {
        return null;
    }
    return KEY_LOGISTICS_CLIENTS.find((client) => client.id === bestClient) || null;
}

function parseQuantityUnit(text) {
    const pattern = /(^|\s)(\d+(?:[.,]\d+)*)\s*(CJ|UN)\b/gi;
    let match;
    while ((match = pattern.exec(text)) !== null) {
        const raw = match[2];
        const unit = match[3].toUpperCase();
        const quantity = Number.parseInt(raw.replace(/[.,]/g, ''), 10);
        if (!Number.isFinite(quantity)) {
            continue;
        }
        return { quantity, unit };
    }
    return null;
}

function extractItemQuantityUnit(lines, startIndex) {
    const direct = parseQuantityUnit(lines[startIndex]);
    if (direct) {
        return direct;
    }
    const maxLookahead = 6;
    let combined = '';
    for (let i = startIndex + 1; i < lines.length && i <= startIndex + maxLookahead; i += 1) {
        const candidate = lines[i];
        if (!candidate) {
            continue;
        }
        if (/\b\d{6,}\b/.test(candidate)) {
            break;
        }
        combined += ` ${candidate}`;
        if (/\b(CJ|UN)\b/i.test(combined)) {
            const parsed = parseQuantityUnit(combined);
            if (parsed) {
                return parsed;
            }
        }
    }
    return null;
}

function pickMappingForCodes(codes, client) {
    if (!codes.length) {
        return null;
    }
    for (const code of codes) {
        const entries = CODE_TO_CLIENT.get(code);
        if (!entries || entries.length === 0) {
            continue;
        }
        if (!client) {
            return { code, mapping: entries[0].mapping };
        }
        const match = entries.find((entry) => entry.clientId === client.id);
        if (match) {
            return { code, mapping: match.mapping };
        }
    }
    return null;
}

function convertToBoxes(quantity, unit, boxSize) {
    if (unit === 'CJ') {
        return quantity;
    }
    if (unit === 'UN') {
        if (!boxSize || quantity % boxSize !== 0) {
            return 0;
        }
        return quantity / boxSize;
    }
    return 0;
}

function parseKeyLogisticsOrderText(text) {
    const quantities = { ...EMPTY_ORDER_QUANTITIES };
    const unknownCodes = [];
    const lines = String(text || '')
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);

    const normalizedText = normalizeText(text);
    const ocNumber = extractOrderNumber(lines);
    const items = [];

    for (let i = 0; i < lines.length; i += 1) {
        const line = lines[i];
        const codes = line.match(/\b\d{6,}\b/g);
        if (!codes || codes.length === 0) {
            continue;
        }
        const quantityUnit = extractItemQuantityUnit(lines, i);
        if (!quantityUnit) {
            continue;
        }
        items.push({
            codes,
            quantity: quantityUnit.quantity,
            unit: quantityUnit.unit
        });
    }

    const allCodes = items.flatMap((item) => item.codes);
    const client = findClientByAlias(normalizedText) || findClientByCodes(allCodes);

    items.forEach((item) => {
        const mappingResult = pickMappingForCodes(item.codes, client);
        if (!mappingResult) {
            if (item.codes.length) {
                unknownCodes.push(...item.codes);
            }
            return;
        }
        const boxes = convertToBoxes(item.quantity, item.unit, mappingResult.mapping.boxSize);
        if (boxes <= 0) {
            return;
        }
        quantities[mappingResult.mapping.key] += boxes;
    });

    const ruts = extractRuts(text);
    const rut = ruts.find((value) => !BLOCKED_RUTS.has(value)) || null;

    return {
        quantities,
        clientId: client ? client.id : null,
        rut,
        ocNumber,
        unknownCodes: Array.from(new Set(unknownCodes))
    };
}

function extractOrderNumber(lines) {
    for (let i = 0; i < lines.length; i += 1) {
        const line = lines[i];
        if (/orden de compra/i.test(line)) {
            const direct = line.match(/\b(\d{6,})\b/);
            if (direct) {
                return direct[1];
            }
            for (let j = i + 1; j < Math.min(i + 4, lines.length); j += 1) {
                const candidate = lines[j].match(/\b(\d{6,})\b/);
                if (candidate) {
                    return candidate[1];
                }
            }
        }
    }
    return null;
}

export {
    parseKeyLogisticsOrderText,
    EMPTY_ORDER_QUANTITIES,
    KEY_LOGISTICS_CLIENTS
};
