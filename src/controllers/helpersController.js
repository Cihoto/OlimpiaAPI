import fs from 'fs';
import csvParser from 'csv-parser';
import OpenAI from 'openai';

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
    const { emailBody, emailSubject } = req.body; // Adjusted to handle JSON request body

    const newBody = JSON.stringify(emailBody).replace(/\\n/g, ' ');
    const newSubject = JSON.stringify(emailSubject).replace(/\\n/g, ' ');

    console.log("newBody", newBody);

    res.status(200).json({ newBody, newSubject });
    return;

    // Sanitize emailBody and emailSubject to remove control characters and unnecessary content
    const sanitizedEmailBody = emailBody
        .replace(/\r?\n|\r/g, ' ') // Replace all line breaks with spaces
        .replace(/---------- Forwarded message ---------/g, '') // Remove forwarded message header
        .replace(/De:.*\n/g, '') // Remove sender information
        .replace(/Date:.*\n/g, '') // Remove date information
        .replace(/Subject:.*\n/g, '') // Remove subject information
        .replace(/To:.*\n/g, '') // Remove recipient information
        .replace(/-- .*/g, '') // Remove signature separator
        .replace(/[\r\n]+/g, ' ') // Replace newlines with spaces
        .trim();

    const sanitizedEmailSubject = emailSubject
        .replace(/Fwd: /g, '') // Remove "Fwd:" prefix
        .replace(/[\r\n]+/g, ' ') // Replace newlines with spaces
        .trim();

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
    Debes analizar el texto del body del correo: "${sanitizedEmailBody}" y el asunto: "${sanitizedEmailSubject}", y deberás extraer los datos relevantes para guardarlos en variables. Nuestro negocio se llama Olimpia SPA y nuestro rut es 77.419.327-8. 
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
    Monto neto: También llamado subtotal. Si es que existe.
    Iva: monto del impuesto. Si es que existe.
    Total: Monto total del pedido, impuestos incluidos. Si es que existe.
    Sender_Email: Es el email de quien envía
    URL_ADDRESS: Dirección de despacho URL encoded, lista para usarse en una petición HTTP GET. No devuelvas nada más que la cadena codificada, sin explicaciones ni comillas.`;

    try {
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

        console.log("valido", validJson.Rut);

        const clientData = await readCSV_private(validJson.Rut, validJson.Direccion_despacho);

        console.log("clientData", clientData);
        const merged = {
            ...validJson,
            ...clientData,
        };

        res.status(200).json({ merged });
        return;

    } catch (error) {
        console.log(error);
        res.status(500).json({ error: 'Error processing the email body', message: error });
    }
}

async function readCSV_not(req, res) {
    const results = [];
    // const {rutToSearch} = req.query; // Get the RUT from the query parameters
    const rutToSearch = "76.865.177-9"; // Get the RUT from the query parameters
    const address = "kennedy 5753 local 02"; // Get the address from the query parameters
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
            .on('end', () => {
                console.log(results);

                const findByAddress = searchByAddress(results, address); // Search by address
                res.status(200).json(results); // Return all data
            });
    } catch (error) {
        res.status(500).json({ error: 'Error reading the CSV file' });
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



async function readCSV_privatee(rutToSearch, address) {
    const results = [];
    // const { rutToSearch, address } = req.query; // Get the RUT from the query parameters
    console.log(`RUT to search: ${rutToSearch}`); // Log the RUT to search
    console.log(`address to search: ${address}`); // Log the address to search
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
                    return{
                        data: [],
                        length: results.length,
                        address: address ? true : false,
                    }
                }

                if (results.length == 1) {
                    // If only one result is found, return it directly
                    return{
                        data: results[0],
                        length: results.length,
                        address: true
                    }
                }

                if (!address) {
                    // If no address is provided, return all results but address as false
                    const first = results[0];
                    first['Dirección Despacho'] = "";
                    return{
                        data: first,
                        length: results.length,
                        address: false
                    }
                }
                

                //map results array for Gpt token limitation
                const clientData = results.map((item, index) => {
                    return {
                        index: index,
                        direccion: item['Dirección Despacho'],
                    }
                });
                const gptResponse = await integrateWithChatGPT(clientData, address); // Integrate with ChatGPT
                console.log(gptResponse)
                if (gptResponse.length == 0) {

                    return{
                        data: gptResponse,
                        length: gptResponse.length,
                        address: address ? true : false,
                    }
                }

                const matched = gptResponse.find((item) => item.match === true);
                const found = results.find((result, index) => {
                    return index == (matched.index)
                });

                if (!found) {
                    console.log("no se encontro nada")
                    return{
                        data: found,
                        length: [found].length,
                        address: address ? true : false,
                        message: "No se encontro nada"
                    }
                    
                }
                // If a match is found, return the matched address
                console.log("final final")
                return{
                    data: found,
                    length: [found].length,
                    address: true,
                    message: "Se encontro una coincidencia",
                };
            });
    } catch (error) {
        return{success: false, error: 'Error reading the CSV file' };
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
                    console.log("***********************************************");

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
                        });
                        return;
                    }

                    if (results.length == 1) {
                        resolve({
                            data: results[0],
                            length: results.length,
                            address: true,
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
                            message: "No se encontro nada",
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