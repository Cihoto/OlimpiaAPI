const saveSale = {
    "documentType": "string", // dato existente
    "firstFolio": 0, // dato existente
    "lastFolio": 0, // dato existente
    "externalDocumentID": "string", // preguntar a defontana para que se usa y como funciona
    "emissionDate": { //datos existentes
        "day": 0,
        "month": 0,
        "year": 0
    },
    "firstFeePaid": { // se calcula en base a la emision comparada con la fecha de vencimiento
        "day": 0,
        "month": 0,
        "year": 0
    },
    "clientFile": "string", // dato existente 
    "contactIndex": "string", // dato existente 
    "rutMandante": "string", // dato existente
    "paymentCondition": "string", // lo tenemos, falta pulir formato
    "sellerFileId": "string", // falta preguntar si es rut empresa o id de vendedor a defontana
    // preguntar por valores 
    "clientAnalysis": {  //resolver dudas de asientos contables con el cliente
        "accountNumber": "string",
        "businessCenter": "string",
        "classifier01": "string",
        "classifier02": "string"
    },
    //EN DOCUMENTACION SE AGREGA SALEANALYSIS PERO EN SWAGGER NO
    "billingCoin": "string", // dato existente
    "billingRate": 0, // dato existente
    "shopId": "string", // dato existente
    "priceList": "string", // preguntar al cliente sobre la configuracion de listas de precios en la plataforma
    "giro": "string", // dato existente, falta preguntar el valor al cliente
    "district": "string", // dato existente
    "city": "string", // dato existente
    "contact": 0, // dato existe por defecto -1
    "attachedDocuments": [ // preguntar al cliente si envian orden de compra junto a factura o solo se ocupa el numero de OC
        {
            "date": {
                "day": 0,
                "month": 0,
                "year": 0
            },
            "documentTypeId": "string",
            "folio": "string",
            "reason": "string"
        }
    ],
    "storage": { // 
        "code": "string", // 
        "motive": "string", // Preguntar al cliente si mueven stock por Defontana
        "storageAnalysis": { // preguntar al cliente por asientos contables de inventario
            "accountNumber": "string",
            "businessCenter": "string",
            "classifier01": "string",
            "classifier02": "string"
        }
    },
    "details": [
        {
            "type": "string", //Dato existente
            "isExempt": false, // dato existente
            "code": "string", // dato existente
            "count": 0, //dato existente
            "productName": "string", // dato existente
            "productNameBarCode": "string", // preguntar al cliente y a defontana por el uso del codigo de barras
            "comment": "string", // preguntar a defontana por este campo, aparece en swagger pero no en documentacion
            "price": 0, // dato existente
            "discount": { // preguntar a cliente si el precio se ve directamente con cliente o si se aplica descuento a clientes especificos
                "type": 0,
                "value": 0
            },
            "unit": "string", //dato existente
            "analysis": { // preguntar al cliente por asientos contables de ventas
                "accountNumber": "string",
                "businessCenter": "string",
                "classifier01": "string",
                "classifier02": "string"
            },
            "useBatch": true, // dato existente
            "batchInfo": [ 
                {
                    "amount": 0,
                    "batchNumber": "string"
                }
            ]
        }
    ],
    "saleTaxes": [
        {
            "code": "IVA",
            "value": 19,
            "taxeAnalysis": { // preguntar al cliente por asientos contables
                "accountNumber": "string",
                "businessCenter": "string",
                "classifier01": "string",
                "classifier02": "string"
            }
        }
    ],
    "ventaRecDesGlobal": [ // preguntar al cliente ,en caso de aplicarse algun descuento, ver si se aplica de forma global o por producto
        {
            "amount": 0,
            "modifierClass": "string", 
            "name": "string",
            "percentage": 0,
            "value": 0
        }
    ],
    "gloss": "string", // preguntar a defontana que corresponde la glosa en comparacion al detalle
    "customFields": [ // preguntasr al cliente sobre los campos . OPCIONAL, CAMPOS PERSONALIZABLES, EN CASO DE NO USAR ENVIAR COMO UN ARREGLO VACÍO []
        {
            "name": "string",
            "value": "string"
        }
    ],
    "isTransferDocument": true // dato existente
}
