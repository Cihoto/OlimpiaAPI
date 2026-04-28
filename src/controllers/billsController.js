import Bill from '../models/Bill.js';
import {
    FACTURADO_STATUS,
    normalizeRut,
    shouldNotifyInvoice,
    buildInvoiceNotificationEmail,
    sendNotificationEmail
} from '../services/invoiceNotificationEmail.js';
import {
    findSentInvoiceNotification,
    saveSentInvoiceNotification
} from '../services/mongoInvoiceNotificationRegistry.js';
import {
    prepareDeliveryReservationForBilling,
    markDeliveryReservationAsCommitted,
    markDeliveryReservationAsFailed
} from '../services/deliveryCapacityService.js';

function pickFirstDefinedValue(candidates) {
    for (const value of candidates) {
        if (value !== undefined && value !== null && value !== '') {
            return value;
        }
    }
    return null;
}

function resolveInvoiceStatus(defontanaResponse) {
    if (!defontanaResponse) {
        return 'error';
    }

    if (defontanaResponse.success === false || defontanaResponse.error) {
        return 'error';
    }

    // Defontana success payload example:
    // { success: true, message: "Venta Guardada Exitosamente", ... }
    if (defontanaResponse.success === true) {
        return FACTURADO_STATUS;
    }

    const statusCandidate = pickFirstDefinedValue([
        defontanaResponse.status,
        defontanaResponse.estado,
        defontanaResponse.documentStatus,
        defontanaResponse.saleStatus,
        defontanaResponse?.data?.status,
        defontanaResponse?.data?.estado
    ]);

    if (!statusCandidate) {
        return 'error';
    }

    const normalizedStatus = String(statusCandidate).toLowerCase().trim();
    if (
        normalizedStatus.includes('factur') ||
        normalizedStatus.includes('issued') ||
        normalizedStatus.includes('emitid') ||
        normalizedStatus === 'ok' ||
        normalizedStatus === 'success'
    ) {
        return FACTURADO_STATUS;
    }

    return normalizedStatus;
}

function resolveInvoiceId({ requestBody, billJson, defontanaResponse }) {
    const invoiceId = pickFirstDefinedValue([
        defontanaResponse?.invoiceId,
        defontanaResponse?.InvoiceId,
        defontanaResponse?.id,
        defontanaResponse?.Id,
        defontanaResponse?.saleId,
        defontanaResponse?.SaleID,
        defontanaResponse?.externalDocumentID,
        defontanaResponse?.externalDocumentId,
        defontanaResponse?.data?.invoiceId,
        defontanaResponse?.data?.id,
        defontanaResponse?.data?.externalDocumentID,
        billJson?.externalDocumentID,
        requestBody?.externalDocumentID,
        requestBody?.invoiceId
    ]);

    return invoiceId ? String(invoiceId) : null;
}

function resolveInvoiceFolio({ requestBody, billJson, defontanaResponse }) {
    const folio = pickFirstDefinedValue([
        defontanaResponse?.folio,
        defontanaResponse?.Folio,
        defontanaResponse?.data?.folio,
        billJson?.firstFolio,
        requestBody?.firstFolio
    ]);

    return folio ?? null;
}

function resolveInvoiceAmount({ requestBody, billJson, defontanaResponse }) {
    const amount = pickFirstDefinedValue([
        defontanaResponse?.total,
        defontanaResponse?.Total,
        defontanaResponse?.amount,
        defontanaResponse?.monto,
        defontanaResponse?.data?.total,
        defontanaResponse?.data?.amount,
        requestBody?.Total,
        requestBody?.Monto
    ]);

    if (amount !== null) {
        return amount;
    }

    if (Array.isArray(billJson?.details)) {
        const total = billJson.details.reduce((acc, item) => {
            const count = Number(item?.count || 0);
            const price = Number(item?.price || 0);
            return acc + (Number.isFinite(count) && Number.isFinite(price) ? count * price : 0);
        }, 0);
        return total;
    }

    return null;
}

function resolveRazonSocial({ requestBody, rutCliente, defontanaResponse }) {
    const razonSocial = pickFirstDefinedValue([
        requestBody?.Razon_social,
        requestBody?.razonSocial,
        requestBody?.razon_social,
        requestBody?.clientName,
        defontanaResponse?.razonSocial,
        defontanaResponse?.clientName,
        defontanaResponse?.data?.razonSocial,
        defontanaResponse?.data?.clientName
    ]);

    if (razonSocial) {
        return razonSocial;
    }

    if (normalizeRut(rutCliente) === normalizeRut('96.930.440-6')) {
        return 'KEYLOGISTICS CHILE S A';
    }

    return 'Sin razon social';
}

async function maybeSendInvoiceNotification({ requestBody, billJson, defontanaResponse }) {
    const status = resolveInvoiceStatus(defontanaResponse);
    const rutCliente = pickFirstDefinedValue([
        requestBody?.clientFile,
        billJson?.clientFile,
        requestBody?.Rut,
        requestBody?.rut
    ]);

    if (!shouldNotifyInvoice({ rut: rutCliente, status })) {
        return {
            attempted: false,
            sent: false,
            duplicate: false,
            status,
            reason: 'skip_rules'
        };
    }

    const invoiceId = resolveInvoiceId({ requestBody, billJson, defontanaResponse });
    if (!invoiceId) {
        return {
            attempted: false,
            sent: false,
            duplicate: false,
            status,
            reason: 'missing_invoice_id'
        };
    }

    const existingNotification = await findSentInvoiceNotification({ invoiceId, status: FACTURADO_STATUS });
    if (existingNotification) {
        return {
            attempted: true,
            sent: false,
            duplicate: true,
            status: FACTURADO_STATUS,
            invoiceId,
            messageId: existingNotification.messageId || null
        };
    }

    const razonSocial = resolveRazonSocial({ requestBody, rutCliente, defontanaResponse });
    const folio = resolveInvoiceFolio({ requestBody, billJson, defontanaResponse });
    const monto = resolveInvoiceAmount({ requestBody, billJson, defontanaResponse });
    const fechaFacturacion = new Date().toISOString();
    const { subject, text } = buildInvoiceNotificationEmail({
        razonSocial,
        rut: rutCliente,
        folio,
        monto,
        fechaFacturacion,
        invoiceId
    });

    const emailResult = await sendNotificationEmail({ subject, text });
    const saveResult = await saveSentInvoiceNotification({
        invoiceId,
        rutCliente,
        messageId: emailResult.messageId,
        status: FACTURADO_STATUS,
        fromEmail: emailResult.fromEmail,
        recipientEmail: emailResult.recipientEmail
    });

    return {
        attempted: true,
        sent: saveResult.inserted === true,
        duplicate: saveResult.duplicate === true,
        status: FACTURADO_STATUS,
        invoiceId,
        ...emailResult
    };
}

async function createBill(req, res) {
    if (!req.apiKey) {
        res.status(401).json({ code: 401, error: 'Error al autenticar' });
        return;
    }
    let deliveryReservationInBilling = null;
    let deliveryReservationStatusPersisted = false;
    let deliveryDayOverwrittenByReservation = false;

    try {
        // const reqq = {
        //     body: {
        //         apiKey : "",
        //         documentType: "FVAELECT",
        //         firstFolio : 0,
        //         lastFolio : 0,
        //         clientFile : "76.322.465-1",
        //         paymentCondition : "30",//
        //         sellerFileId : "17511433-5",
        //         businessCenter: "VNT", // VNT O CNP (VENTA O CONCEPCION)
        //         shopId : "Local",
        //         priceList : "1",
        //         giro : "Mi giro comercial",
        //         attachedDocuments : [
        //             {
        //                 folio: 123,
        //                 documentType: 801,
        //                 date: moment().format("DD-MM-YYYY"),
        //             }
        //         ],
        //         storage : "BODEGACENTRAL",
        //         details : [
        //             {
        //                 code : "003",
        //                 quantity : 1,
        //                 price: 0  //mandar en 0 en para usar precio en lista
        //             },
        //             {
        //                 code : "17798147780052",
        //                 quantity : 1,
        //                 price: 0 
        //             }
        //         ],
        //         ventaRecDesGlobal : [],
        //         customFields: [],
        //         gloss : "esta es la glosa del documento",
        //         isTransferDocument : true
        //     }
        // };
        // const {body} = reqq;


        // FROM HERE

        const { body } = req;
        let reservationReference = null;
        if (body.reservationId !== undefined && body.reservationId !== null && body.reservationId !== '') {
            reservationReference = String(body.reservationId).trim();
        } else if (
            body.deliveryReservation !== undefined &&
            body.deliveryReservation !== null &&
            body.deliveryReservation !== ''
        ) {
            if (typeof body.deliveryReservation === 'string') {
                const trimmedReservationValue = body.deliveryReservation.trim();
                if (trimmedReservationValue.startsWith('{')) {
                    try {
                        reservationReference = JSON.parse(trimmedReservationValue);
                    } catch (error) {
                        throw {
                            code: 400,
                            error: 'Bad request',
                            message: 'deliveryReservation tiene formato invalido'
                        };
                    }
                } else {
                    reservationReference = trimmedReservationValue;
                }
            } else {
                reservationReference = body.deliveryReservation;
            }
        }

        if (reservationReference) {
            const preparedReservation = await prepareDeliveryReservationForBilling({
                deliveryReservation: reservationReference
            });

            deliveryReservationInBilling = preparedReservation?.deliveryReservation || null;
            if (!deliveryReservationInBilling?.reservationId) {
                throw {
                    code: 400,
                    error: 'Bad request',
                    message: 'reservationId es requerido y debe existir en Mongo'
                };
            }

            const reservedDeliveryDay = deliveryReservationInBilling?.assignedDeliveryDay || null;
            if (
                reservedDeliveryDay &&
                String(body.deliveryDay || '') !== String(reservedDeliveryDay)
            ) {
                body.deliveryDay = reservedDeliveryDay;
                deliveryDayOverwrittenByReservation = true;
            }
        }

        const bill = new Bill();
        bill.apiKey = req.apiKey
        bill.documentType = body.documentType;
        bill.firstFolio = body.firstFolio;
        bill.lastFolio = body.lastFolio;
        bill.clientFile = body.clientFile;
        bill.paymentCondition = body.paymentCondition;
        bill.sellerFileId = body.sellerFileId;
        bill.billingCoin = body.billingCoin;
        bill.businessCenter = body.businessCenter;
        bill.shopId = body.shopId;
        bill.priceList = body.priceList;
        bill.giro = body.giro;
        bill.attachedDocuments = body.attachedDocuments;
        bill.storage = body.storage;//pendiente de verificacion con defontana y OLIMPIA
        bill.details = body.details;//pendiente de verificacion con defontana y OLIMPIA
        bill.ventaRecDesGlobal = body.ventaRecDesGlobal;
        bill.customFields = body.customFields;
        bill.gloss = body.gloss;
        bill.customFields = body.customFields;
        bill.isTransferDocument = body.isTransferDocument;
        bill.deliveryDay = body.deliveryDay ? body.deliveryDay : null;

        bill.validate();
        const BILLJSON = await bill.toJSON();

        console.log("+++++++++++++++++++++++ ++++++++++++++++++++++++++++++++ +++++++++++++++++++++++");
        console.log("+++++++++++++++++++++++ JSON DE DOCUMENTO PARA DEFONTANA +++++++++++++++++++++++");
        console.log("+++++++++++++++++++++++ ++++++++++++++++++++++++++++++++ +++++++++++++++++++++++");
        console.log("BILLJSON", BILLJSON);

        let createBillDefontanaResponse = null;

        //Agregar producto a BILLJSON.details en caso de que solo se compre una caja de un solo producto
        let prodQtyToAdd = 0;
        BILLJSON.details.forEach(item => {
            console.log("item.count", item);
            prodQtyToAdd += item.count;
        });

        console.log("prodQtyToAdd", prodQtyToAdd);

        if (prodQtyToAdd === 1 && (body.isDelivery === true || body.isDelivery === "true")) {
            const normalizedRegion = String(body.region || '').toUpperCase().trim();
            console.log("Agregando costo de despacho segun region:", body.region);
            if (normalizedRegion === "RM") {
                BILLJSON.details.push({
                    "type": "A",
                    "isExempt": false,
                    "code": "70724043633538",
                    "count": 1,
                    "productName": "DESPACHO RM",
                    "productNameBarCode": "",
                    "price": 5000,
                    "discount": {
                        "type": 0,
                        "value": 0
                    },
                    "unit": "UN",
                    "analysis": {
                        "accountNumber": "3110101001",
                        "businessCenter": "EMPNEGVTAVTA000",
                        "classifier01": "",
                        "classifier02": ""
                    },
                    "useBatch": false,
                    "batchInfo": []
                });
            }

            if (normalizedRegion === "V" || normalizedRegion === "VI") {
                BILLJSON.details.push({
                    "type": "A",
                    "isExempt": false,
                    "code": "70724043633553",
                    "count": 1,
                    "productName": "DESPACHO V y VI",
                    "productNameBarCode": "",
                    "price": 10000,
                    "discount": {
                        "type": 0,
                        "value": 0
                    },
                    "unit": "UN",
                    "analysis": {
                        "accountNumber": "3110101001",
                        "businessCenter": "EMPNEGVTAVTA000",
                        "classifier01": "",
                        "classifier02": ""
                    },
                    "useBatch": false,
                    "batchInfo": []
                });
            }
        }

        // let existingBills = [];
        // if (fs.existsSync(filePath)) {
        //     const fileContent = fs.readFileSync(filePath, 'utf-8');
        //     existingBills = JSON.parse(fileContent);
        // }

        // existingBills.push(BILLJSON);

        // fs.writeFileSync(filePath, JSON.stringify(existingBills, null, 2), 'utf-8');

        console.log(BILLJSON);

        // Comentar fetch a Defontana para pruebas sin consumir cupo y simular respuesta exitosa de facturación
        const saveSaleURL = `${process.env.SALE_API_URL}SaveSale`
        console.log("saveSaleURL", saveSaleURL);
        const createBillDefontana = await fetch(saveSaleURL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${req.apiKey}`
            },
            body: JSON.stringify(BILLJSON)
        });

        createBillDefontanaResponse = await createBillDefontana.json();
        console.log("createBillDefontanaResponse", createBillDefontanaResponse);

        const invoiceStatus = resolveInvoiceStatus(createBillDefontanaResponse);

        // Por ahora, simular que la factura se emite correctamente en Defontana para probar flujo de reservas de entrega y notificaciones sin consumir cupo real en Defontana.
        // const invoiceStatus = FACTURADO_STATUS;
        // TODO: Manejar estado PENDING_VERIFY sin consumir cupo cuando Defontana quede incierto.
        if (deliveryReservationInBilling?.reservationId) {
            if (invoiceStatus === FACTURADO_STATUS) {
                const committedReservation = await markDeliveryReservationAsCommitted({
                    deliveryReservation: deliveryReservationInBilling,
                    defontanaResponse: createBillDefontanaResponse
                });
                deliveryReservationInBilling = committedReservation?.deliveryReservation || deliveryReservationInBilling;
            } else {
                const failedReservation = await markDeliveryReservationAsFailed({
                    deliveryReservation: deliveryReservationInBilling,
                    reason: 'defontana_error',
                    defontanaResponse: createBillDefontanaResponse
                });
                deliveryReservationInBilling = failedReservation?.deliveryReservation || deliveryReservationInBilling;
            }
            deliveryReservationStatusPersisted = true;
        }

        let notificationEmail;
        try {
            notificationEmail = await maybeSendInvoiceNotification({
                requestBody: body,
                billJson: BILLJSON,
                defontanaResponse: createBillDefontanaResponse
            });
        } catch (error) {
            console.error('Error sending invoice notification email:', error);
            return res.status(500).json({
                success: false,
                error: 'No se pudo enviar correo de notificacion',
                details: error.message || String(error),
                deliveryReservation: deliveryReservationInBilling
            });
        }

        const responsePayload = {
            success: true,
            data: BILLJSON,
            notificationEmail,
            deliveryReservation: deliveryReservationInBilling
        };

        if (createBillDefontanaResponse) {
            responsePayload.createBillDefontanaResponse = createBillDefontanaResponse;
        }
        responsePayload.deliveryDayOverwrittenByReservation = deliveryDayOverwrittenByReservation;

        res.status(200).json(responsePayload);

        return;

        // const checkIssuedBillURL = `https://replapi.defontana.com/api/Sale/GetSaleByExternalDocumentID?externalDocumentID=1101997304`
        // const checkIssuedBill = await fetch(checkIssuedBillURL,{
        //     method: 'GET',
        //     headers:{
        //         ContentType: 'application/json',    
        //         Authorization: `Bearer ${req.apiKey}`
        //     }
        // })

        // const checkIssuedBillResponse = await checkIssuedBill.json();

        // console.log("checkIssuedBillResponse", checkIssuedBillResponse);

        // res.json(checkIssuedBillResponse);

        // const {detailsList} = BILLJSON;

        // const mapped = detailsList.map(item => item.code)

        // console.log("mapped", mapped);
    } catch (error) {
        if (deliveryReservationInBilling?.reservationId && !deliveryReservationStatusPersisted) {
            try {
                const failedReservation = await markDeliveryReservationAsFailed({
                    deliveryReservation: deliveryReservationInBilling,
                    reason: error?.message || 'create_bill_exception'
                });
                deliveryReservationInBilling = failedReservation?.deliveryReservation || deliveryReservationInBilling;
            } catch (reservationError) {
                console.error('Error updating delivery reservation after createBill failure:', reservationError);
            }
        }

        console.error('Error creating bill:', error);
        if (error.code && error.message) {
            res.status(error.code).json({
                ...error,
                success: false,
                deliveryReservation: deliveryReservationInBilling
            });
        } else {
            res.status(500).json({
                errorCode: 5000,
                errorMessage: 'Internal server error',
                deliveryReservation: deliveryReservationInBilling
            });
        }
    }
}
async function getBillById(req, res) {
    if (!req.apiKey) {
        res.status(401).json({ code: 401, error: 'Error al autenticar solicitud' });
        return;
    }
    try {
        const { billId } = req.params;
        const checkIssuedBillURL = `${process.env.SALE_API_URL}GetSaleByExternalDocumentID?externalDocumentID=${billId}`
        const checkIssuedBill = await fetch(checkIssuedBillURL, {
            method: 'GET',
            headers: {
                ContentType: 'application/json',
                Authorization: `Bearer ${req.apiKey}`
            }
        })

        const checkIssuedBillResponse = await checkIssuedBill.json();
        console.log("checkIssuedBillResponse", checkIssuedBillResponse);
        res.json(checkIssuedBillResponse);

    } catch (error) {
        console.log(error);
        return res.status(500).json({ errorCode: 5000, errorMessage: 'Internal server error' });
    }
}

async function preflightBill(req, res) {
    if (!req.apiKey) {
        res.status(401).json({ code: 401, error: 'Error al autenticar' });
        return;
    }
    try {
        const { body } = req;
        const bill = new Bill();
        bill.apiKey = req.apiKey;
        bill.documentType = body.documentType;
        bill.firstFolio = body.firstFolio;
        bill.lastFolio = body.lastFolio;
        bill.clientFile = body.clientFile;
        bill.paymentCondition = body.paymentCondition;
        bill.sellerFileId = body.sellerFileId;
        bill.billingCoin = body.billingCoin;
        bill.businessCenter = body.businessCenter;
        bill.shopId = body.shopId;
        bill.priceList = body.priceList;
        bill.giro = body.giro;
        bill.attachedDocuments = body.attachedDocuments;
        bill.storage = body.storage;
        bill.details = body.details;
        bill.ventaRecDesGlobal = body.ventaRecDesGlobal;
        bill.customFields = body.customFields;
        bill.gloss = body.gloss;
        bill.isTransferDocument = body.isTransferDocument;
        bill.deliveryDay = body.deliveryDay ? body.deliveryDay : null;

        const report = await bill.runPreflight();

        res.status(report.allPassed ? 200 : 400).json({
            success: report.allPassed,
            summary: {
                allPassed: report.allPassed,
                passedCount: report.passedCount,
                failedCount: report.failedCount,
                firstFailure: report.firstFailure
            },
            steps: report.steps
        });
    } catch (error) {
        console.error('Error running bill preflight:', error);
        res.status(500).json({ errorCode: 5000, errorMessage: 'Internal server error during preflight' });
    }
}

export { createBill, getBillById, preflightBill };
