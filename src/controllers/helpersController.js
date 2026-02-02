import fs from 'fs';
import path from 'path';
import csvParser from 'csv-parser';
import OpenAI from 'openai';
import moment from 'moment';
import XLSX from 'xlsx';
import pdfParse from 'pdf-parse';
import findDeliveryDayByComuna from '../utils/findDeliveryDate.js'; // Import the function to find delivery day by comuna
import foundSpecialCustomers from '../services/foundSpecialCustomers.js';
import { analyzeOrderEmail, analyzeOrderEmailFromGmail } from '../services/analyzeOrderEmail.js'; // Import the function to analyze order email
import { parseKeyLogisticsOrderText, EMPTY_ORDER_QUANTITIES } from '../services/keyLogisticsOrderParser.js';
import {
    findProcessedKeyLogisticsOrder,
    insertProcessedKeyLogisticsOrder
} from '../services/mongoOrderRegistry.js';
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
        if (k.includes('razon') || k.includes('razon social') || k.includes('razon_social') || k.includes('razon social')) return 'RAZÓN SOCIAL';
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

async function readEmailBody(req, res) {

    const plainText = req.body;
    try {
        // console.log("hola",req.body)
        // console.log("Received plainText:", plainText);

        // Sanitize the email body
        const sanitizedEmailBody = plainText
            .replaceAll(/\s+/g, ' ') // Remove all white spaces
            .trim(); // Trim leading and trailing spaces

        const {
            emailBody,
            emailSubject,
            emailAttached,
            emailDate,
            source,
            keyLogistics
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
Ejemplo: si el documento dice "Dirección: NUEVA LOS LEONES 030" y "Dirección: AVDA COSTANERA SUR 2710", devolver ["NUEVA LOS LEONES 030", "AVDA COSTANERA SUR 2710"]
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
        const sanitizedOutput = jsonResponse.replace(/```json|```/g, '').replace(/\n/g, '').replace(/\\/g, '');
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
        if (keyLogisticsData?.rut) {
            validJson.Rut = keyLogisticsData.rut;
        }
        if (keyLogisticsData) {
            const normalizedRut = validJson.Rut ? normalizeRut(validJson.Rut) : '';
            if (normalizedRut && KEY_LOGISTICS_BLOCKED_RUTS.has(normalizedRut)) {
                validJson.Rut = null;
            }
        }

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

        const analyzeOrderEmaiResponse = keyLogisticsData?.quantities
            ? { ...EMPTY_ORDER_QUANTITIES, ...keyLogisticsData.quantities }
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
            const resultado = await readCSV_private(validJson.Rut, direccion, validJson.precio_caja, validJson.isDelivery, emailDate);
            
            // Verificar si encontramos datos válidos (no array vacío y tiene Región Despacho)
            const regionDespachoTemp = resultado?.data?.['Región Despacho'];
            const esValido = resultado.data && 
                             !Array.isArray(resultado.data) && 
                             typeof regionDespachoTemp === 'string' && 
                             regionDespachoTemp.trim() !== '';
            
            if (esValido) {
                clientData = resultado;
                direccionUsada = direccion;
                console.log(`✓ Coincidencia encontrada con dirección: ${direccion}`);
                break;
            } else {
                console.log(`✗ Sin coincidencia para dirección: ${direccion}`);
            }
        }
        
        // Si no encontramos nada con ninguna dirección, usar el resultado del último intento o hacer uno con la principal
        if (!clientData) {
            clientData = await readCSV_private(validJson.Rut, validJson.Direccion_despacho, validJson.precio_caja, validJson.isDelivery, emailDate);
        }
        
        console.log("clientData", clientData);
        console.log("clientData Región Despacho", clientData.data?.['Región Despacho']);
        console.log("Dirección usada para match:", direccionUsada);
        console.log("{}{}{}{}{}{}{}{}{}{}{}{}{}{}}{{}}{{}}{}{}{}{}{}{}{}{}{");
        
        // Verificar si clientData.data existe, no es array, y tiene 'Región Despacho' como string válido
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

        };

        res.status(200).json({ merged });
        return;

    } catch (error) {
        console.log(error);

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
        const keyLogisticsSender = 'fax@keylogistics.cl';
        const allowedSenders = new Set([
            'compras.marketds@pedidosya.com',
            keyLogisticsSender
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

        if (sender === keyLogisticsSender) {
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

            return res.status(200).json({
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
                    source: 'gmail'
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

        return res.status(200).json({
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

//integracion con chat gpt

async function integrateWithChatGPT(addresses, targetAddress) {

    const prompt = `Busca dentro de este arreglo ${JSON.stringify(addresses)} la mejor coincidencia para la dirección "${targetAddress}".
    En caso de encontrar una coincidencia, devolver un array JSON con el objeto que contenga la dirección agregando "match": true.
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

async function readCSV_private(rutToSearch, address, boxPrice, isDelivery, emailDate) {
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


                        const deliveryDay = findDeliveryDayByComuna(results[0]['Comuna Despacho'], emailDate);
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
                            message: "Cliente no encontrado en base de clientes por falta de dirección",
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
                            message: "No se puede encontrar coincidencias por falta de dirección",
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
                    const deliveryDay = findDeliveryDayByComuna(found['Comuna Despacho'], emailDate);

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


export { readCSV, readEmailBody, readEmailBodyFromGmail };
