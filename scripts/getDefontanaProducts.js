/**
 * Script para obtener todos los productos de Defontana
 * Incluye autenticación automática para obtener el API Key
 * 
 * Uso: node scripts/getDefontanaProducts.js
 */

import 'dotenv/config';

const DEFONTANA_API_URL = process.env.SALE_API_URL || 'https://replapi.defontana.com/api/Sale/';
const AUTH_API_URL = 'https://replapi.defontana.com/api/';

// ============================================
// AUTENTICACIÓN - Obtener API Key fresco
// ============================================
const getApiKey = async () => {
    if (!AUTH_API_URL) {
        throw new Error('La variable API_URL no está definida en el archivo .env');
    }

    const fullUrl = `${AUTH_API_URL}Auth?client=${process.env.ID_CLIENTE}&company=${process.env.ID_EMPRESA}&user=${process.env.ID_USUARIO}&password=${process.env.PASSWORD}`;
    
    console.log('🔐 Autenticando con Defontana...');
    console.log(`   URL: ${AUTH_API_URL}`);
    
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
            throw new Error(`Error al obtener la API key: ${response.statusText}`);
        }

        const data = await response.json();

        if (data.success) {
            console.log('✅ API Key obtenida exitosamente');
            return data.access_token;
        } else {
            throw new Error('La respuesta no indica éxito al obtener la API key.');
        }
    } catch (error) {
        console.error('❌ Error al autenticar:', error.message);
        throw error;
    }
};

// ============================================
// PAGINACIÓN
// ============================================
const getPagination = () => {
    return {
        itemsPerPage: 100,
        pageNumber: 1,
        totalItems: 0,
        totalPossibleIterations: 0
    };
};

// ============================================
// OBTENER PRODUCTOS
// ============================================
const getProducts = async (apiKey) => {
    try {
        if (!apiKey) {
            return {
                success: false,
                message: "API_KEY no disponible"
            };
        }

        let paginationInfo = getPagination();
        // status: 0=inactivos, 1=activos, 2=todos
        const productsURL = `${DEFONTANA_API_URL}Getproducts?status=1&itemsPerPage=${paginationInfo.itemsPerPage}&pageNumber=1`;
        
        console.log(`\n📦 Consultando productos (página 1): ${productsURL}`);
        
        const prods = await fetch(productsURL, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${apiKey}`
            }
        });

        const productsData = await prods.json();

        if (!productsData.success) {
            console.log('📥 Respuesta de la API:', JSON.stringify(productsData, null, 2));
            return {
                success: false,
                message: productsData.exceptionMessage || productsData.message || "No hay productos disponibles"
            };
        }

        if (productsData.totalItems === 0) {
            return {
                success: true,
                totalProducts: 0,
                productList: []
            };
        }

        // Obtener total de items y calcular iteraciones necesarias
        paginationInfo.totalItems = productsData.totalItems;
        paginationInfo.totalPossibleIterations = Math.ceil(productsData.totalItems / paginationInfo.itemsPerPage);

        console.log(`📊 Total de productos encontrados: ${paginationInfo.totalItems}`);
        console.log(`📄 Páginas totales a consultar: ${paginationInfo.totalPossibleIterations}`);

        let allProducts = productsData.productList || [];
        console.log(`   ✓ Página 1: ${allProducts.length} productos obtenidos`);

        // Si hay más de una página, obtener el resto
        if (paginationInfo.totalPossibleIterations > 1) {
            for (let i = 2; i <= paginationInfo.totalPossibleIterations; i++) {
                console.log(`   Obteniendo página ${i} de ${paginationInfo.totalPossibleIterations}...`);
                
                const paginatedURL = `${DEFONTANA_API_URL}Getproducts?status=1&itemsPerPage=${paginationInfo.itemsPerPage}&pageNumber=${i}`;
                const prods = await fetch(paginatedURL, {
                    method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${apiKey}`
                    }
                });

                const pageData = await prods.json();

                if (!pageData.success) {
                    console.log(`   ⚠️ Error en página ${i}: ${pageData.message}`);
                    continue; // Continuar con la siguiente página en caso de error
                }
                
                const pageProducts = pageData.productList || [];
                allProducts = [...allProducts, ...pageProducts];
                console.log(`   ✓ Página ${i}: ${pageProducts.length} productos obtenidos (Total acumulado: ${allProducts.length})`);
            }
        }

        return { 
            success: true, 
            totalProducts: allProducts.length,
            totalItemsReported: paginationInfo.totalItems,
            pagesProcessed: paginationInfo.totalPossibleIterations,
            productList: allProducts 
        };
    } catch (error) {
        console.error('Error:', error);
        return {
            success: false,
            message: "Error al obtener los productos: " + error.message
        };
    }
};

// ============================================
// EJECUTAR SCRIPT
// ============================================
const main = async () => {
    console.log('='.repeat(50));
    console.log('🚀 Obteniendo productos de Defontana');
    console.log('='.repeat(50));
    
    try {
        // 1. Obtener API Key fresco
        const apiKey = await getApiKey();
        
        // 2. Obtener todos los productos
        const result = await getProducts(apiKey);
        
        if (result.success) {
            console.log('\n✅ Productos obtenidos exitosamente');
            console.log(`📦 Total productos obtenidos: ${result.totalProducts}`);
            console.log(`📊 Total reportado por API: ${result.totalItemsReported}`);
            console.log(`📄 Páginas procesadas: ${result.pagesProcessed}`);
            
            if (result.productList.length > 0) {
                console.log('\n📋 Primeros 5 productos:');
                console.log(JSON.stringify(result.productList.slice(0, 5), null, 2));
            }
            
            // Guardar todos los productos en un archivo JSON con metadata
            const fs = await import('fs');
            const outputData = {
                exportDate: new Date().toISOString(),
                totalProducts: result.totalProducts,
                totalItemsReported: result.totalItemsReported,
                pagesProcessed: result.pagesProcessed,
                products: result.productList
            };
            
            fs.writeFileSync('productos_defontana.json', JSON.stringify(outputData, null, 2));
            console.log('\n💾 Todos los productos guardados en: productos_defontana.json');
        } else {
            console.log('\n❌ Error:', result.message);
        }
    } catch (error) {
        console.error('\n❌ Error fatal:', error.message);
    }
};

main();

