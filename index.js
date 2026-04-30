import express from 'express';
import dotenv from 'dotenv';
import bodyParser from 'body-parser';
import { authMiddleware } from './src/middleware/auth.js';
import {
  getAllCoinsId, getAllPaymentsConditions, getShops, getPriceList, getPriceListDetails, getDocumentAnalysis, getClients,
  getClientByFileId, sellersId, getProds, getStorages, businessAnalysis, paymentConditions, businessCenters, classifierAnalysis
} from './src/utils/sales.js';
import billsRoutes from './src/routes/bills.js';
import helpersRoutes from './src/routes/helpers.js';
import botRoutes from './src/routes/botRoutes.js';
import devRoutes from './src/routes/dev.js';
import orderRoutes from './src/routes/orderRoutes.route.js';
// import googleRoutes from './src/routes/google.js';
import morgan from 'morgan';
import Bill from './src/models/Bill.js';
import cors from 'cors';
import bannerRoutes from './src/routes/bannerRouter.js';
import { fileURLToPath } from 'url';
import moment from 'moment';
import { startDeliveryCapacityCleanupCron } from './src/services/deliveryCapacityService.js';
import { syncKnowledgebase } from './src/services/sheetsSyncService.js';

import findDeliveryDayByComuna from './src/utils/findDeliveryDate.js'; // Import the function
// moment.tz.setDefault('America/Santiago'); // Set default timezone to Chile's timezone

const app = express();
dotenv.config();

// Middlewares
app.use(bodyParser.json({ limit: '25mb' }));
// Middleware para procesar texto plano
app.use(express.text({ limit: '25mb' }));
app.use(morgan('dev'));
app.use(cors());

// Auth middleware
app.use(authMiddleware);

app.use('/api', billsRoutes);
app.use('/helpers', helpersRoutes);
app.use('/bot', botRoutes);
app.use('/dev', devRoutes);

// app.use('/google',googleRoutes)
// Routes
app.get('/', async (req, res) => {

  if (!req.apiKey) {
    return res.status(500).json({ error: 'Error al autenticar la solicitud' });
  }
  const { apiKey } = req;

  
  // const BILLJSON = {
  //   "aaaa": "string"
  // }
  // const saveSaleURL = `${process.env.SALE_API_URL}SaveSale`
  // console.log("saveSaleURL", saveSaleURL);
  // const createBillDefontana = await fetch(saveSaleURL, {
  //   method: 'POST',
  //   headers: {
  //     'Content-Type': 'application/json',
  //     Authorization: `Bearer ${req.apiKey}`
  //   },
  //   body: JSON.stringify(BILLJSON)
  // });

  // const createBillDefontanaResponse = await createBillDefontana.json();
  // console.log("createBillDefontanaResponse", createBillDefontanaResponse);
  // res.status(200).json({
  //   createBillDefontanaResponse,
  //   success: true,
  //   data: BILLJSON
  // });
  // const { apiKey } = req;
  // const clientURL = `https://api.defontana.com/api/Sale/GetClientsByFileID?fileId=77.732.169-2&status=1&itemsPerPage=10&pageNumber=1`;
  // const client = await fetch(clientURL, {
  //   method: 'GET',
  //   headers: {
  //     'Content-Type': 'application/json',
  //     'Authorization': `Bearer ${apiKey}`
  //   }
  // });
  // const clientData = await client.json();

  // res.json({ apiKey: req.apiKey,clientData:clientData });
  // return


  // res.json({message:"Hello World, this is your api token " + req.apiKey});

  // const emailDate =  "2025-05-13T15:57:46.000Z"

  // const date = moment(emailDate).tz('America/Santiago').format('YYYY-MM-DDTHH:mm:ssZ');
  // console.log("date", date);
  // res.json({date});
  // return

  const communityResponse = findDeliveryDayByComuna("Providencia", "2025-11-05T08:55:59.000Z");
  // 26-05-2025 09:57:50
  // const communityResponse = findDeliveryDayByComuna("EL BOSQUE", "2025-06-28T08:57:50Z");
  // console.log("communityResponse", communityResponse);
  // res.json(communityResponse);
  // return;
  // return

  // if(!req.apiKey) {
  //   return res.status(500).json({ error: 'Error al autenticar la solicitud' });
  // }

  // const allCoins = await getAllCoinsId(req.apiKey)
  const allPayments = await getAllPaymentsConditions(req.apiKey)
  // const shops = await getShops(req.apiKey)
  // const priceList = await getPriceList(req.apiKey)
  // const getPriceListDetail = await getPriceListDetails(req.apiKey)
  // const documentAnalysis = await getDocumentAnalysis(req.apiKey)
  // const getClientss = await getClients(req.apiKey) // api/Sale/GetClients
  // const getClientFromId = await getClientByFileId(req.apiKey, "76995636-0")
  // const sellersIdd = await sellersId(req.apiKey) 
  // const getProdss = await getProds(req.apiKey)
  // const getStoragess = await getStorages(req.apiKey)
  // const businessAnalysiss = await businessAnalysis(req.apiKey);// api/Sale/GetDocumentAnalisys
  // const codess = allPayments.items.map((desc) => desc.code);
  res.json({ allPayments });
  return


  // const businessCentersURL = `${process.env.ACCOUNTING_API_URL_DEV}GetBusinessCenterPlan`
  // const businessCenters = await fetch(businessCentersURL, {
  //   method: 'GET',
  //   headers: {
  //     'Content-Type': 'application/json',
  //     'Authorization': `Bearer ${apiKey}`
  //   }
  // });
  // const businessCentersData = await businessCenters.json();
  // console.log("businessCentersData", businessCentersData);
  // res.json({ businessCentersData });


  // const classifierAnalysisURL = `${process.env.ACCOUNTING_API_URL_DEV}GetAccountAnalisys?Account=EMPNEGVTAVTA000`;
  // const classifierAnalysis = await fetch(classifierAnalysisURL, {
  //   method: 'GET',
  //   headers: {
  //     'Content-Type': 'application/json',
  //     'Authorization': `Bearer ${apiKey}`
  //   }
  // });
  // const classifierAnalysisData = await classifierAnalysis.json();
  // res.json({ classifierAnalysisData });
  return
  const businessCenter = await businessCenters(req.apiKey) //preguntar por API //api/Accounting/GetBusinessCenterPlan
  const saleBusinessCenterAccounts = {
    defaultSale: "EMPNEGVTAVTA000",
    CNPSale: "EMPNEGVTACCP000"
  }
  const classifierAnalysiss = await classifierAnalysis(req.apiKey, saleBusinessCenterAccounts.defaultSale) //preguntar por API //api/Accounting/GetBusinessCenterPlan
  // const paymentConditionss = await  paymentConditions(req.apiKey); // api/Sale/GetPaymentConditions

  res.json(businessCenter)
  return
  // businessCenterLogic
  const { centrosNegocios } = businessCenter;

  const expectedBusinessCenter = centrosNegocios.find(desc => desc.code === "EMP000000000000")
    ?.descendientes.find((desc) => {
      return desc.code === "EMPNEG000000000";
    })
    ?.descendientes.find((desc) => {
      return desc.code === "EMPNEGVTA000000";
    })?.descendientes

  const codes = centrosNegocios.map((desc) => desc.code);
  console.log(businessCenter);

  res.json(expectedBusinessCenter);
  return


  // res.json({businessCenter});  
  // return
  const bill = new Bill();
  bill.apiKey = req.apiKey;
  // bill.shopId = "1";
  // bill.priceList = "1";
  bill.clientFile = "76.322.465-1";
  const response = await bill.getFileid();
  // const reponse = await bill.getPriceList();
  res.json(response);
  return

  // res.json(moneyTypesData);

  res.send('Hello World, this is your api token ' + req.apiKey);

});

app.use('/api/banner', bannerRoutes);
app.use('/api/orders', orderRoutes);




const deliveryCleanupIntervalMinutes = Number(process.env.DELIVERY_HOLD_CLEANUP_INTERVAL_MINUTES || 12);
startDeliveryCapacityCleanupCron({
  intervalMinutes: deliveryCleanupIntervalMinutes,
  runOnStart: true,
  logger: console
});

// Start server
const PORT = process.env.PORT || 5000;
app.listen(5000, () => {
  console.log(`Server is running on port ${PORT}`);

  // Sync periódico Sheet → MongoDB — DESACTIVADO INDEFINIDAMENTE (2026-04-30)
  // Para reactivar: descomentar el bloque y revisar que SHEETS_SYNC_INTERVAL_HOURS sea válido.
  // El sync manual sigue disponible vía POST /helpers/sync-knowledgebase.
  // const syncIntervalHours = Number(process.env.SHEETS_SYNC_INTERVAL_HOURS || 0);
  // if (syncIntervalHours > 0) {
  //   const intervalMs = syncIntervalHours * 60 * 60 * 1000;
  //   console.log(`[sheetsSync] Sync automático configurado cada ${syncIntervalHours}h`);
  //   setInterval(async () => {
  //     try {
  //       const stats = await syncKnowledgebase();
  //       console.log('[sheetsSync] Sync periódico completado:', stats);
  //     } catch (err) {
  //       console.error('[sheetsSync] Error en sync periódico:', err.message);
  //     }
  //   }, intervalMs);
  // }
  console.log('[sheetsSync] Cron automático DESACTIVADO indefinidamente. Sync manual: POST /helpers/sync-knowledgebase');
});
