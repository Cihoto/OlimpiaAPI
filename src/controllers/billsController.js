import Bill from '../models/Bill.js';
import moment from 'moment';

// ...existing code...

async function createBill(req, res) {
    if(!req.apiKey) {
        res.status(401).json({code:401, error: 'Error al autenticar' });
        return;
    }
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

        const {body} = req;
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


        bill.validate();
        const BILLJSON = await bill.toJSON();
        console.log(BILLJSON);
        res.status(200).json({
            success: true,
            data: BILLJSON
        });
        return

        // res.status(200).json(BILLJSON);

        const saveSaleURL = `https://replapi.defontana.com/api/sale/SaveSale`
        const createBillDefontana = await fetch(saveSaleURL,{
            method: 'POST',
            headers:{
                'Content-Type': 'application/json',    
                Authorization: `Bearer ${req.apiKey}`
            },
            body: JSON.stringify(BILLJSON)
        })

        const createBillDefontanaResponse = await createBillDefontana.json();
        console.log("createBillDefontanaResponse", createBillDefontanaResponse);

        res.status(200).json(createBillDefontanaResponse);

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



        return

        
    } catch (error) {
        console.error('Error creating bill:', error);
        if (error.code && error.message) {
            res.status(error.code).json({	
                ...error,
                success:false
            });
        } else {
            res.status(500).json({ errorCode: 5000, errorMessage: 'Internal server error' });
        }
    }
}

// ...existing code...

export { createBill };