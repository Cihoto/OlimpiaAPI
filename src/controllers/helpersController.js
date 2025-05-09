import fs from 'fs';
import csvParser from 'csv-parser';
import OpenAI from 'openai';
import moment from 'moment';
import findDeliveryDayByComuna from '../utils/findDeliveryDate.js'; // Import the function to find delivery day by comuna
import foundSpecialCustomers from '../services/foundSpecialCustomers.js';

const client = new OpenAI();

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

                results.forEach((item) => {
                    // Normalize prices to numbers
                    item['Precio Caja'] = item['Precio Caja'].replaceAll('.', '');
                    item['Precio Caja'] = Number(item['Precio Caja']);
                    // item['Precio Caja'] = item['Precio Caja'].replaceAll('$', '');
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
            .trim(); // Trim leading and trailing spaces

        const {emailBody, emailSubject, emailAttached} = JSON.parse(sanitizedEmailBody); // Parse the sanitized email body

        console.log(JSON.parse(sanitizedEmailBody));
        // if (emailBody == null || emailSubject == null || emailAttached == null) {
        //     console.log("Invalid request emailBody:", emailBody);
        //     console.log("Invalid request emailSubject:", emailSubject);
        //     console.log("Invalid request emailAttached:", emailAttached);
        //     // Return an error response if any of the required fields are missing

        //     return res.status(400).json({ error: 'Invalid request body' });
        // }

        const requiredFields = ['emailBody', 'emailSubject', 'emailAttached'];

        const missingFields = requiredFields.filter(field => !(field in JSON.parse(sanitizedEmailBody)));

        if (missingFields.length > 0) {
            console.log("Invalid request, missing fields:", missingFields);
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
        Rut: Contiene el Rut del cliente, debes buscarlo en el body del correo y en el asunto si no existe devuelve "null".
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
        precio_caja: Precio de la caja de chocolate pink, amargo o leche. Si no existe, devuelve 0.
        URL_ADDRESS: Dirección de despacho URL encoded, lista para usarse en una petición HTTP GET. No devuelvas nada más que la cadena codificada, sin explicaciones ni comillas.
        PaymentMethod:{
            method: En caso de hacer referencia a un cheque, devolver letra C. En caso de no hacer referencia a un cheque, devolver ""
            paymentsDays: Devolver el número de días de pago. En caso de no hacer referencia a un cheque, devolver "".
        }
        isDelivery: En caso de que el pedido sea para delivery, devolver true. En caso de que no sea para delivery, devolver false.
        
        Las variables "Pedido_Cantidad_Pink", "Pedido_Cantidad_Amargo" y "Pedido_Cantidad_Leche" deben ser números enteros, teniendo en consideracion todas estos puntos:
        -Todas las cajas contienen 24 unidades.
        -Si el pedido es multiplo de 24, debes diferenciar si se refiere a 24 unidades o 1 caja. Un caso practico es: 
            -24 x24 unidades, refiere a 24 cajas de 24 unidades, por lo tanto la cantidad de cajas es 24.
            -24 cajas x24 unidades, refiere a 24 cajas de 24 unidades, por lo tanto la cantidad de cajas es 24.
            -48 unidades, refiere a 2 cajas de 24 unidades, por lo tanto la cantidad de cajas es 2.
            -24 uds, refiere a 1 caja de 24 unidades, por lo tanto la cantidad de cajas es 1.
            Estas son otras formas de entender la tarea:
            -Si el pedido solo menciona unidades y el numero es multiplo de 24, se debe dividir por 24 para obtener la cantidad de cajas, ejemplos:
                -48 unidades de chocolate pink, equivale a 2 cajas de chocolate pink.
                -22 unidades de chocolate pink, equivale a 22 cajas de chocolate pink. 
                -24 unidades de chocolate pink, equivale a 1 caja de chocolate pink.
            -En caso de tener la mencionar cajas, se debe hacer referencia a la cantidad de cajas, no a las unidades. Ejemplo:
                -21 cajas de chocolate, equivale a 21 cajas de chocolate.
                -24 cajas de chocolate, equivale a 24 caja de chocolate.
                -48 cajas de chocolate, equivale a 48 cajas de chocolate.
                *estos tres ultimos casos son solo cuando se mencione que quieren cajas de chocolate pink, no unidades.       
        -Reconocer si el pedido es por cajas o por unidades (por ejemplo: 1 caja de chocolate pink o 24 unidades de chocolate pink).
        -En caso de encontrar N caja/s x 24 unidades solo se debe hacer referencia a la cantidad de cajas, no a las unidades.
        -En caso de que el detalle del pedido solo haga referencia a una cantidad de unidades de chocolate pink, leche o amargo, se debe dividir por 24 para obtener la cantidad de cajas.
        -Siempre se considera que la venta es por cajas, no por unidades.
        -OLIMPIA SPA no vende unidades, solo cajas de 24 unidades. no se hacen ventas por unidades ejemplo (23 unidades de una caja)
        
        Para la variable rut debes considerar lo siguiente:
        -Puede tener los siguientes formatos:
            - xx.xxx.xxx-x
            - xxx.xxx.xxx-x
            - xxxxxxxx-x
            - xx.xxx.xxx-x
        -El rut puede estar tanto en el inicio como en el final del correo.
        -El rut puede estar en el asunto o en el cuerpo del correo.
        -Tener en cuenta que en caso de encontrar el rut de Olimpia SPA, se debe seguir buscando el rut en el correo, ya que estamos buscando el rut del cliente, no el de la empresa.
        -Este campo es sumamente importante, sin este dato la ejecucion del bot no es valida.
        
        Razon_social:
        -La razon social puede estar en el cuerpo del correo o en el asunto.
        -En caso de no haber una indicacion de enviar a una razon social especifica, podria mencionarse sucursal, local o razon social.

        Direccion_despacho:
        -La dirección de despacho puede estar en el cuerpo del correo o en el asunto.
        -En caso de no haber una indicacion de enviar a una direccion especifica, podria mencionarse sucursal, local o direccion de despacho.
        -Debes extraer toda la direccion, incluyendo la comuna y el nombre de la calle.
        -Todas las direcciones pertenecen al territorio chileno por lo que las comunas son chilenas.
        
        precio_caja:
        -El precio de la caja de chocolate pink, amargo o leche. Si no existe, devuelve 0.
        -Debes extraer el precio de la caja, ronda entre los $60000 y $80000 aproximadamente(los valores fluctuan entre clientes por lo que no siempre es el mismo).
        -El precio por caja es el mismo para todos los productos.
        -en caso de que no hayan precios, devolver 0.

        isDelivery:
        -En caso de que el pedido sea para delivery, devolver true. En caso de que no sea para delivery, devolver false.
        -Se debe determinar si el pedido requiere despacho o tendrá modalidad de retiro en sucursal.
        -Casos de retiro (solo ejemplos):
            *Te quiero hacer un pedido de 1 caja de Franui dulce y 1 caja de Franui amargo,
             para retirar este viernes 9 de mayo
            *pedido con retiro
        -debes buscar en todo el texto buscando patrones que indiquen que el pedido es para retiro en sucursal o posee modalidad de despacho.
        -En caso de no quedar claro si el pedido es para retiro o despacho, devolver true para ir a buscar la direccion de despacho
        -Cuando el pedido es para retiro, debes cambiar el valor de direccion despacho a "RETIRO".
        `

        const response = await client.chat.completions.create({
            model: 'gpt-4o',
            messages: [
                { role: 'system', content: systemPrompt },
                { role: 'user', content: userPrompt },
            ]
        });

        const jsonResponse = response.choices[0].message.content.trim();
        const sanitizedOutput = jsonResponse.replace(/```json|```/g, '').replace(/\n/g, '').replace(/\\/g, '');
        const validJson = JSON.parse(sanitizedOutput)[0];

        console.log("***********************************************VALID JSON *************************************************");
        console.log({validJson});
        
        let rutIsFound = false
        if(!validJson.Rut || validJson.Rut == "null" || validJson.Rut == "" || validJson.Rut == "undefined" || validJson.Rut == null || validJson.Rut == undefined || validJson.Rut == "N/A") {
            const foundSpecialCustomer =   foundSpecialCustomers(validJson.Razon_social);
            if(foundSpecialCustomer){
                validJson.Rut = foundSpecialCustomer;
                rutIsFound = true
            }
        }else{
            rutIsFound = true
        }

        console.log("****************************************RUT IS FOUND *************************************************");
        console.log("rutIsFound", rutIsFound);


        // if(!validJson.Rut || validJson.Rut == "null" || validJson.Rut == "" || validJson.Rut == "undefined" || validJson.Rut == null || validJson.Rut == undefined || validJson.Rut == "N/A") {
        if(rutIsFound == false){ 

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

        const clientData = await readCSV_private(validJson.Rut, validJson.Direccion_despacho, validJson.precio_caja, validJson.isDelivery); // Call the readCSV function with the RUT and address
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

async function readCSV_private(rutToSearch, address, boxPrice, isDelivery) {
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
                    console.log("***********************************************")
                    console.log("results", results);
                    
                    results.forEach((item) => {
                        // Normalize prices to numbers

                        item['Precio Caja'] = item['Precio Caja'].replaceAll(',', '');
                        item['Precio Caja'] = item['Precio Caja'].replaceAll('.', '');
                        item['Precio Caja'] = item['Precio Caja'].replaceAll('$', '');
                        item['Precio Caja'] = Number(item['Precio Caja']);
                    });

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
                        const deliveryDay = findDeliveryDayByComuna(results[0]['Comuna Despacho']);
                        if (deliveryDay) {
                            results[0]['deliveryDay'] = deliveryDay;
                        }else{
                            results[0]['deliveryDay'] = "";
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
                    if(isDelivery == false && results.length > 1){
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
                    console.log({gptResponse});

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

                    const deliveryDay = findDeliveryDayByComuna(found['Comuna Despacho']);

                    if (deliveryDay) {
                        found['deliveryDay'] = deliveryDay;
                    }else{
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


export { readCSV, readEmailBody };