import { OpenAI } from 'openai';

const SYSTEM_INSTRUCTIONS = ''; // Instrucciones del sistema (puedes personalizarlas después)
const OLIMPIA_ASSISTANT_ID = process.env.OLIMPIA_ORDER_FINDER_ASSISTENT_ID; // ID del asistente en OpenAI Platform
const OPENAI_OLIMPIA_API_KEY = process.env.OPENAI_OLIMPIA; // Clave API de OpenAI

const GMAIL_ORDER_ANALYSIS_PROMPT = `Devuelveme exclusivamente un JSON, sin explicaciones ni texto adicional
No incluyas ningun texto antes o despues del JSON.
No uses formato Markdown.
No expliques lo que estas haciendo.

Olimpia SPA maneja dos formatos de productos:

Productos de 150 gramos: vienen en cajas de 24 unidades
Productos de 90 gramos: vienen en cajas de 18 unidades
Reglas para interpretar cantidad de cajas:
Siempre debes entregar la cantidad en cajas, no en unidades.
Si el pedido menciona caja o cajas, usa directamente ese numero como la cantidad de cajas.

=== REGLAS PARA PRODUCTOS DE 150 GRAMOS (24 unidades por caja) ===
Aplica a: Amargo, Leche, Pink y Free (cuando NO especifican 90g)

Ejemplos:
1 caja de chocolate pink equivale a 1,
24 cajas equivale a 24,
48 cajas x 24 unidades equivale a 48,

Si el pedido menciona solo unidades (unidades, uds, unidades de) y el numero es multiplo de 24, divide por 24 para obtener la cantidad de cajas.
Ejemplos:
48 unidades de chocolate pink equivale a 2,
24 uds equivale a 1,
72 unidades equivale a 3,

Si el pedido no menciona que es en cajas, se asume que esta expresado en unidades y hay que dividirlas.
Si el pedido menciona una cantidad que no es multiplo de 24 y no dice que son cajas, la cantidad es invalida, devuelve 0.
Ejemplos:
23 unidades de chocolate equivale a 0,
25 uds de leche equivale a 0,

Si el texto menciona algo como 24 x 24 unidades o 24 cajas x 24 unidades, interpreta que se trata de 24 cajas, no multipliques por 24.

=== REGLAS PARA PRODUCTOS DE 90 GRAMOS (18 unidades por caja) ===
Aplica a: Amargo 90g, Leche 90g, Pink 90g (cuando especifican 90g o 90 gramos)

Ejemplos:
1 caja de Franui Leche 90g equivale a 1,
18 cajas de Pink 90 gramos equivale a 18,

Si el pedido menciona solo unidades y el numero es multiplo de 18, divide por 18 para obtener la cantidad de cajas.
Ejemplos:
36 unidades de Franui Leche 90g equivale a 2,
18 uds de Pink 90 gramos equivale a 1,
54 unidades de Amargo 90g equivale a 3,

Si el pedido menciona una cantidad que no es multiplo de 18 y no dice que son cajas, la cantidad es invalida, devuelve 0.
Ejemplos:
17 unidades de Leche 90g equivale a 0,
19 uds de Pink 90 gramos equivale a 0,

=== REGLA POR DEFECTO ===
Si el pedido NO especifica gramos (90g, 90 gramos), se asume que es el producto de 150 gramos (24 unidades por caja).
Ejemplos:
Franui Leche (sin especificar) = Producto de 150g, usar regla de 24 unidades
Franui Leche 90g = Producto de 90g, usar regla de 18 unidades

Ejemplos adicionales productos 150g:
48 unidades de chocolate pink equivale a 2 cajas,
24 cajas de chocolate amargo equivale a 24 cajas,
96 uds de leche equivale a 4 cajas,
23 unidades de chocolate pink equivale a 0,
24 x 24 unidades equivale a 24 cajas,
2 cajas de chocolate amargo equivale a 2 cajas,
3 cajas de Franui Free equivale a 3 cajas,

Ejemplos adicionales productos 90g:
36 unidades de Franui Leche 90g equivale a 2 cajas,
18 unidades de Pink 90 gramos equivale a 1 caja,
2 cajas de Amargo 90g equivale a 2 cajas,

Formas de llamar a las cajas:
cajas, cjas, cjs, cj, display.
Estos ejemplos pueden estar en mayusculas o minusculas.

=== PRODUCTOS DE 150 GRAMOS (24 unidades por caja) ===

Pedido_Cantidad_Amargo (150g):
BOMBONES FRANUÃ­ DE FRAMBUESA EN CHOCOLATE AMARGO Y BLANCO 150 G
CHO NEG-BLAN
BLANCO-NEGRO
FRAMB CHO NEG-BLAN S/GLU
Franui Negro
FRANUI AMARGO 150G
Franui Amargo
Caja Franui Amargo

Pedido_Cantidad_Leche (150g):

FRAMB CHO LECH-BLA S/GLU 1X24U
BOMBONES FRANUÃ­ DE FRAMBUESA CON CHOCOLATE DE LECHE Y BLANCO 150 G
CHOC BLAN-LECH
Frambuesas Banadas De Chocolate De Leche Y Chocolate Blanco
Franui Dulce
Franui Leche
Caja Franui Leche

Pedido_Cantidad_Pink (150g):
BOMBONES FRANUI PINK FRAMBUESAS CON CHOCOLATE BLANCO 150 G
FRAMB CHO PINK
Franui Pink
Caja Franui Pink

Pedido_Cantidad_Free (150g, 24 unidades, sin azucar):
BOMBONES FRANUÃ­ FREE SIN GLUTEN 150 G
Franui Chocolate Free
Franui Free
Franui Chocolate Free
Caja Franui Free
Chocolate Free

=== PRODUCTOS DE 90 GRAMOS (18 unidades por caja) ===

Pedido_Cantidad_Amargo_90g:

Caja Franui Amargo 90 gramos
Franui Amargo 90g
Franui Amargo 90
Amargo 90g
FRAMB CHO NEG-BLAN 90G

Pedido_Cantidad_Leche_90g:

Caja Franui Leche 90 gramos
Franui Leche 90g
Franui Leche 90
Leche 90g
FRAMB CHO LECH 90G

Pedido_Cantidad_Pink_90g:

Caja Franui Pink 90 gramos
Franui Pink 90g
Franui Pink 90
Pink 90g
FRAMB CHO PINK 90G

Si el nombre del producto esta seguido de una linea con un numero y la palabra CJ (caja), entonces asocia esa cantidad al producto mencionado en la linea anterior.

IMPORTANTE: Si el producto NO especifica "90g" o "90 gramos", se asume que es el producto de 150 gramos.

Los valores deben ser numericos.

Devuelve este JSON:

{
"Pedido_Cantidad_Pink": 0,
"Pedido_Cantidad_Amargo": 0,
"Pedido_Cantidad_Leche": 0,
"Pedido_Cantidad_Free": 0,
"Pedido_Cantidad_Pink_90g": 0,
"Pedido_Cantidad_Amargo_90g": 0,
"Pedido_Cantidad_Leche_90g": 0
}`;

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

function parsePedidosYaExcelText(text) {
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

    if (descIdx === -1 || (cajasIdx === -1 && unidadesIdx === -1)) {
        return null;
    }

    for (let i = headerIndex + 1; i < lines.length; i += 1) {
        const row = lines[i].split('\t');
        if (row.length <= descIdx) {
            continue;
        }

        const description = row[descIdx];
        const key = classifyProduct(description);
        if (!key) {
            continue;
        }

        const cajas = cajasIdx !== -1 && row.length > cajasIdx ? parseNumber(row[cajasIdx]) : 0;
        const unidades = unidadesIdx !== -1 && row.length > unidadesIdx ? parseNumber(row[unidadesIdx]) : 0;

        let boxes = cajas;
        if (!boxes && unidades) {
            const boxSize = key.includes('_90g') ? 18 : 24;
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

function parsePedidosYaOrderQuantities(emailContent) {
    try {
        const payload = JSON.parse(emailContent);
        if (!payload || typeof payload !== 'object') {
            return null;
        }
        const emailAttached = payload.emailAttached;
        if (!emailAttached || typeof emailAttached !== 'string') {
            return null;
        }
        return parsePedidosYaExcelText(emailAttached);
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
        const thread = await openai.beta.threads.create();
        const threadId = thread.id;

        let activeRun;
        do {
            const runs = await openai.beta.threads.runs.list(threadId);
            activeRun = runs.data.find(run => run.status === 'active');

            if (activeRun) {
                console.log(`⏳ Esperando a que termine el run activo: ${activeRun.id}`);
                await new Promise(resolve => setTimeout(resolve, 1000)); // Espera 1 segundo
            }
        } while (activeRun);

        // Agrega mensaje del usuario al thread
        await openai.beta.threads.messages.create(threadId, {
            role: "user",
            content: emailContent
        });

        // Ejecuta el assistant
        const run = await openai.beta.threads.runs.create(threadId, {
            assistant_id: OLIMPIA_ASSISTANT_ID
        });

        // Espera que termine el procesamiento
        let runStatus;
        do {
            await new Promise(resolve => setTimeout(resolve, 1000));
            runStatus = await openai.beta.threads.runs.retrieve(threadId, run.id);
        } while (runStatus.status !== 'completed');

        // Recupera la última respuesta del bot
        const messages = await openai.beta.threads.messages.list(threadId);
        const assistantResponse = messages.data.find(m => m.role === 'assistant');
        console.log('assistantResponse', assistantResponse);
        const reply = parseToJson(assistantResponse?.content?.[0]?.text?.value || 'Sin respuesta');;
        console.log("reply", reply);
        return reply;
    } catch (error) {
        console.error("Error analyzing order email:", error);
        return {
            Pedido_Cantidad_Pink: 0,
            Pedido_Cantidad_Amargo: 0,
            Pedido_Cantidad_Leche: 0,
            Pedido_Cantidad_Free: 0,
            Pedido_Cantidad_Pink_90g: 0,
            Pedido_Cantidad_Amargo_90g: 0,
            Pedido_Cantidad_Leche_90g: 0
        }
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

export { analyzeOrderEmail, analyzeOrderEmailFromGmail, parsePedidosYaOrderQuantities };
