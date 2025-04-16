import fs from 'fs';
import csvParser from 'csv-parser';
import OpenAI from 'openai';
import moment from 'moment';

const client = new OpenAI();

const CSV = './src/documents/CLIENTES_OLIMPIA.csv'; // Use the file path as a string

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

                results.forEach((item) => {
                    // Normalize prices to numbers
                    item['Precio Caja'] = item['Precio Caja'].replaceAll(',', '');
                    item['Precio Caja'] = item['Precio Caja'].replaceAll('$', '');
                    item['Precio Caja'] = Number(item['Precio Caja']);
                })

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
    try{


        // console.log("hola",req.body)
        // console.log("Received plainText:", plainText);
            
        // Sanitize the email body
        const sanitizedEmailBody = plainText
            .replaceAll(/\s+/g, ' ') // Remove all white spaces
            .trim();
        
        console.log("Sanitized email body:", sanitizedEmailBody); // Log the sanitized email body
        console.log("Sanitized email body:", sanitizedEmailBody); // Log the sanitized email body

        const {emailBody, emailSubject, emailAttached} = JSON.parse(sanitizedEmailBody); // Parse the sanitized email body

        console.log(JSON.parse(sanitizedEmailBody));
        if (!emailBody || !emailSubject || emailAttached === undefined) {
            return res.status(400).json({ error: 'Invalid request body' });
        }
        let attachedPrompt = ""
        let OC = ""
        if(emailAttached !== ""){
            attachedPrompt = `y el texto que hemos extraido desde un PDF adjunto que trae la orden de compra con el pedido: "${emailAttached}". `
        }


        const systemPrompt = `Devuélveme exclusivamente un JSON válido, sin explicaciones ni texto adicional.
        La respuesta debe comenzar directamente con [ y terminar con ].
        No incluyas ningún texto antes o después del JSON.
        No uses formato Markdown. 
        No expliques lo que estás haciendo.
        Tu respuesta debe ser solamente el JSON. Nada más.;`;

        const userPrompt = `Eres un bot que analiza pedidos para franuí, empresa que comercializa frambuezas bañadas en chocolate. Franuí maneja solamente 3 productos, Frambuezas bañadas en:
        - Chocolate Amargo
        - Chocolate de Leche (tradicional)
        - Chocolate Pink
        Debes analizar el texto del body del correo: "${emailBody}", el asunto: "${emailSubject}" ${attachedPrompt} , y deberás extraer los datos relevantes para guardarlos en variables. 
        Nuestro negocio se llama Olimpia SPA y nuestro rut es 77.419.327-8, por lo tanto ninguna de las variables que extraigas debe contener la palabra Olimpia o nuestro RUT.
        
        Debes extraer los datos del cliente y los datos del pedido para guardarlos en las siguientes variables:
        Razon_social: Contiene la razón social del cliente.
        Direccion_despacho: Dirección a la cual se enviarán los productos. Si no la encuentras, devuelve "null".
        Comuna: Comuna de despacho. Si no la encuentras, devuelve "null".
        Rut: Contiene el Rut del cliente, si no existe devuelve "null".
        Pedido_Cantidad_Pink: Contiene la cantidad de unidades de pedido de chocolate pink. Si es que existe. Si no existe devuelve 0.
        Pedido_Cantidad_Amargo: Contiene la cantidad de unidades de pedido de chocolate amargo. Si es que existe. Si no existe devuelve 0.
        Pedido_Cantidad_Leche: Contiene la cantidad de unidades de pedido de chocolate de leche. Si es que existe. Si no existe devuelve 0.
        Pedido_PrecioTotal_Pink: es el monto total del pedido de chocolate pink, si es que existe. Si no existe, devuelve 0.
        Pedido_PrecioTotal_Amargo: es el monto total del pedido de chocolate amargo, si es que existe. Si no existe devuelve 0.
        Pedido_PrecioTotal_Leche: es el monto total del pedido de chocolate de leche, si es que existe. Si no existe devuelve 0.
        Orden_de_Compra: es el número de orden de compra. Si no existe, devuelve "null".
        Monto neto: También llamado subtotal. Si es que existe.
        Iva: monto del impuesto. Si es que existe.
        Total: Monto total del pedido, impuestos incluidos. Si es que existe.
        Sender_Email: Es el email de quien envía
        URL_ADDRESS: Dirección de despacho URL encoded, lista para usarse en una petición HTTP GET. No devuelvas nada más que la cadena codificada, sin explicaciones ni comillas.
        PaymentMethod:{
            method: En caso de hacer referencia a un cheque, devolver letra C. En caso de no hacer referencia a un cheque, devolver ""
            paymentsDays: Devolver el número de días de pago. En caso de no hacer referencia a un cheque, devolver "".
        } 
        
        Las variables "Pedido_Cantidad_Pink", "Pedido_Cantidad_Amargo" y "Pedido_Cantidad_Leche" deben ser números enteros, teniendo en consideracion todas estos puntos:
        -Todas las cajas contienen 24 unidades.
        -Si el pedido es multiplo de 24, debes diferenciar si se refiere a 24 unidades o 1 caja. Un caso practico es: 
            -24 x24 unidades, refiere a 24 cajas de 24 unidades, por lo tanto la cantidad de cajas es 24.
            -24 cajas x24 unidades, refiere a 24 cajas de 24 unidades, por lo tanto la cantidad de cajas es 24.
            -48 unidades, refiere a 2 cajas de 24 unidades, por lo tanto la cantidad de cajas es 2.
        -Reconocer si el pedido es por cajas o por unidades (por ejemplo: 1 caja de chocolate pink o 24 unidades de chocolate pink).
        -En caso de encontrar N caja/s x 24 unidades solo se debe hacer referencia a la cantidad de cajas, no a las unidades.
        -En caso de que el detalle del pedido solo haga referencia a una cantidad de unidades de chocolate pink, leche o amargo, se debe dividir por 24 para obtener la cantidad de cajas.
        -Siempre se considera que la venta es por cajas, no por unidades.`
        

    
        const response = await client.chat.completions.create({
            model: 'gpt-4o-mini',
            messages: [
                { role: 'system', content: systemPrompt },
                { role: 'user', content: userPrompt },
            ]
        });

        const jsonResponse = response.choices[0].message.content.trim();
        const sanitizedOutput = jsonResponse.replace(/```json|```/g, '').replace(/\n/g, '').replace(/\\/g, '');
        const validJson = JSON.parse(sanitizedOutput)[0];

        console.log(validJson);

        if (!validJson.Rut) {
            console.log("invalido", validJson.Rut);
        }

        // console.log("valido", validJson.Rut);
        if(!validJson.Rut || validJson.Rut == "null" || validJson.Rut == "" || validJson.Rut == "undefined" || validJson.Rut == null || validJson.Rut == undefined || validJson.Rut == "N/A") {

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
        const clientData = await readCSV_private(validJson.Rut, validJson.Direccion_despacho);

        console.log("clientData", clientData);
        const merged = {
            "EmailData": { ...validJson },
            "ClientData": { ...clientData },
            "executionDate" : moment().format('DD-MM-YYYY HH:mm:ss'),
            "OC_date": moment().format('DD-MM-YYYY')
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
            "Pedido_PrecioTotal_Pink": "[null]  Pedido_PrecioTotal_Pink",
            "Pedido_PrecioTotal_Amargo": "[null]  Pedido_PrecioTotal_Amargo",
            "Pedido_PrecioTotal_Leche": "[null]  Pedido_PrecioTotal_Leche",
            "Orden_de_Compra": "[null]  Orden_de_Compra",
            "Monto neto": "[null]  Monto",
            "Iva": "[null]  Iva",
            "Total": "[null]  Total",
            "Sender_Email": "[null]  Sender_Email",
            "URL_ADDRESS": "[null]  URL_ADDRESS"
        }

        res.status(500).json({                
            success: false, 
            error: 'No se ha podido procesar el correo', 
            requestBody: req.body,
            data: response, 
            executionDate: moment().format('DD-MM-YYYY HH:mm:ss'),
            OC_date: moment().format('DD-MM-YYYY')
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

async function readCSV_private(rutToSearch, address) {
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
                    if (data.RUT == normalizedRut) {
                        results.push(data);
                    } // Collect all rows
                })
                .on('end', async () => {

                    results.forEach((item) => {
                        // Normalize prices to numbers
                        item['Precio Caja'] = item['Precio Caja'].replaceAll(',', '');
                        item['Precio Caja'] = item['Precio Caja'].replaceAll('$', '');
                        item['Precio Caja'] = Number(item['Precio Caja']);
                    });

                    if (results.length == 0) {
                        resolve({
                            data: [],
                            length: results.length,
                            address: address ? true : false,
                            message: "Cliente no encontrado en base de clientes",
                        });
                        return;
                    }

                    if (results.length == 1) {
                        resolve({
                            data: results[0],
                            length: results.length,
                            address: true,
                            message: "Cliente encontrado en base de clientes",
                        });
                        return;
                    }

                    if (!address) {
                        const first = results[0];
                        first['Dirección Despacho'] = "";
                        resolve({
                            data: first,
                            length: results.length,
                            address: false,
                            message: "Cliente no encontrado en base de clientes por falta de dirección",
                        });
                        return;
                    }

                    // Map results array for GPT token limitation
                    const clientData = results.map((item, index) => {
                        return {
                            index: index,
                            direccion: item['Dirección Despacho'],
                        };
                    });

                    const gptResponse = await integrateWithChatGPT(clientData, address); // Integrate with ChatGPT
                    console.log(gptResponse);

                    if (gptResponse.length == 0) {
                        resolve({
                            data: gptResponse,
                            length: gptResponse.length,
                            address: address ? true : false,
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
                        });
                        return;
                    }
                    
                    console.log("final final");
                    resolve({
                        data: found,
                        length: [found].length,
                        address: true,
                        message: "Se encontro una coincidencia",
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






export { readCSV, readEmailBody };