const saveSale = {
    "documentType": "string", // preguntar por valores
    "firstFolio": 0, // 0 para electronico
    "lastFolio": 0, // 0 para electronico
    "externalDocumentID": "string", // preguntar por valores no aparece en la documentacion pero si en swagger
    "emissionDate": {
        "day": 0,
        "month": 0,
        "year": 0
    },
    "firstFeePaid": {
        "day": 0,
        "month": 0,
        "year": 0
    },
    "clientFile": "string", // rut del cliente
    "contactIndex": "string", // direccion del cliente
    "rutMandante": "string", // preguntar por que aparece en swagger pero no en la documentacion
    "paymentCondition": "string", // donde poder agregar nuevas condiciones de pago
    "sellerFileId": "string", // que es el identificador del vendedor (rut?)
    // preguntar por valores 
    "clientAnalysis": { 
        "accountNumber": "string",
        "businessCenter": "string",
        "classifier01": "string",
        "classifier02": "string"
    },
    //EN DOCUMENTACION SE AGREGA SALEANALYSIS PERO EN SWAGGER NO
    "billingCoin": "string", // preguntar por valores (CLP, USD, EUR)
    "billingRate": 0, // 1 para peso, preguntar otras opciones
    "shopId": "string", // donde obtengo el id de la tienda || PROBLEMAS DE PERMISOS CON LA API
    "priceList": "string", // donde obtengo la lista de precios
    "giro": "string", // cuales son los valores predeterminados para el giro
    "district": "string", // preguntar por valores
    "city": "string", // preguntar por valores no aparece en la documentacion pero si en swagger
    "contact": 0, // preguntar por valores (numero de contacto de la empresa)
    "attachedDocuments": [ // los docuemntos deben estar creados en erp defontana antes de ser adjuntados?
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
        "code": "string",
        "motive": "string",
        "storageAnalysis": {
            "accountNumber": "string",
            "businessCenter": "string",
            "classifier01": "string",
            "classifier02": "string"
        }
    },
    "details": [
        {
            "type": "string",
            "isExempt": true, // aparece en swagger pero no en documentacion
            "code": "string",
            "count": 0,
            "productName": "string",
            "productNameBarCode": "string",
            "comment": "string", // aparece en swagger pero no en documentacion
            "price": 0,
            "discount": { // aparece en swagger pero no en documentacion
                "type": 0,
                "value": 0
            },
            "unit": "string", //Existen otros valores predeterminados que no sea UN?
            "analysis": { // preguntar por valores
                "accountNumber": "string",
                "businessCenter": "string",
                "classifier01": "string",
                "classifier02": "string"
            },
            "useBatch": true, // aparece en swagger pero no en documentacion
            "batchInfo": [ // aparece en swagger pero no en documentacion
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
            "taxeAnalysis": { // aparece en swagger pero no en documentacion
                "accountNumber": "string",
                "businessCenter": "string",
                "classifier01": "string",
                "classifier02": "string"
            }
        }
    ],
    "ventaRecDesGlobal": [ // donde registro este tipo de descuentos
        {
            "amount": 0,
            "modifierClass": "string", 
            "name": "string",
            "percentage": 0,
            "value": 0
        }
    ],
    "gloss": "string", // glosa o comentario de la factura
    "customFields": [
        {
            "name": "string",
            "value": "string"
        }
    ],
    "isTransferDocument": true
}
