/**
 * Script para obtener y mostrar el API Key de Defontana
 * Usa las mismas credenciales que el middleware de autenticación
 */

import 'dotenv/config';

async function getDefontanaApiKey() {
    if (!process.env.API_URL) {
        console.error('❌ La variable API_URL no está definida en el archivo .env');
        return null;
    }

    const fullUrl = `${process.env.API_URL}Auth?client=${process.env.ID_CLIENTE}&company=${process.env.ID_EMPRESA}&user=${process.env.ID_USUARIO}&password=${process.env.PASSWORD}`;
    
    console.log('Consultando:', fullUrl);
    
    const options = {
        method: 'GET',
        headers: {
            'accept': 'text/plain',
        },
    };

    try {
        const response = await fetch(fullUrl, options);

        if (!response.ok) {
            const errorText = await response.text();
            console.error('❌ Error en la respuesta:', errorText);
            return null;
        }

        const data = await response.json();

        if (data.success) {
            return data.access_token;
        } else {
            console.error('❌ La respuesta no indica éxito');
            return null;
        }
    } catch (error) {
        console.error('❌ Error al realizar la solicitud:', error.message);
        return null;
    }
}

// Ejecutar
const main = async () => {
    console.log('='.repeat(50));
    console.log('Obteniendo API Key de Defontana...');
    console.log('='.repeat(50));
    
    const apiKey = await getDefontanaApiKey();
    
    if (apiKey) {
        console.log('\n✅ API Key obtenida exitosamente:\n');
        console.log(apiKey);
        console.log('\n📋 Copia esta línea para tu .env:');
        console.log(`DEFONTANA_API_KEY=${apiKey}`);
    }
};

main();
