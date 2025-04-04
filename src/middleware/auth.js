import 'dotenv/config'; // Cargar variables del .env

let apiKey = null; // Variable para almacenar la API key

async function fetchApiKey() {
    if (!process.env.API_URL) {
        throw new Error('La variable API_URL no está definida en el archivo .env');
    }

    const fullUrl = `${process.env.API_URL}Auth?client=${process.env.ID_CLIENTE}&company=${process.env.ID_EMPRESA}&user=${process.env.ID_USUARIO}&password=${process.env.PASSWORD}`;
    
    const options = {
        method: 'GET',
        headers: {
            'accept': 'text/plain',
        },
    };
    // console.log('Fetching API key from:', fullUrl); // Log the URL being fetched
    // console.log('Request headers:', options.headers); // Log request headers
    // console.log('Request method:', options.method); // Log request method
    // console.log('Request body:', options.body); // Log request body (if any)

    try {
        const response = await fetch(fullUrl, options);
        // console.log('Response status:', response.status); // Log response status

        if (!response.ok) {
            const errorText = await response.text(); // Log response body for debugging
            console.error('Response body:', errorText);
            throw new Error(`Error al obtener la API key: ${response.statusText}`);
        }

        const data = await response.json();
        // console.log('Response data:', data); // Log the parsed response

        if (data.success) {
            apiKey = data.access_token;
        } else {
            throw new Error('La respuesta no indica éxito al obtener la API key.');
        }
    } catch (error) {
        console.error('Error al realizar la solicitud:', error);
        throw error;
    }
}

function getApiKey() {
    if (!apiKey) {
        throw new Error('La API key no está disponible');
    }
    return apiKey;
}

async function authMiddleware(req, res, next) {
    try {
        if (!apiKey) {
            await fetchApiKey();
        }
        req.apiKey = apiKey; // Agregar la API key al objeto de la solicitud
        next(); // Continuar con el siguiente middleware
    } catch (error) {
        res.status(500).json({ error: 'Error al autenticar la solicitud' });
    }
}

export { fetchApiKey, getApiKey, authMiddleware };
