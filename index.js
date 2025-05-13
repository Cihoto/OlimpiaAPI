import express from 'express';
import dotenv from 'dotenv';
import bodyParser from 'body-parser';
import { authMiddleware } from './src/middleware/auth.js';
import { getAllCoinsId,getAllPaymentsConditions,getShops, getPriceList,getPriceListDetails ,getDocumentAnalysis,getClients,
  getClientByFileId,sellersId,getProds,getStorages, businessAnalysis,paymentConditions,businessCenters,classifierAnalysis
} from './src/utils/sales.js';
import billsRoutes from './src/routes/bills.js';
import helpersRoutes from './src/routes/helpers.js';
import botRoutes from './src/routes/botRoutes.js';
import devRoutes from './src/routes/dev.js';
// import googleRoutes from './src/routes/google.js';
import morgan from 'morgan';
import Bill from './src/models/Bill.js';
import cors from 'cors';
import bannerRoutes from './src/routes/bannerRouter.js';
import { fileURLToPath } from 'url';
import moment from 'moment';

import findDeliveryDayByComuna from './src/utils/findDeliveryDate.js'; // Import the function
moment.tz.setDefault('America/Santiago'); // Set default timezone to Chile's timezone

const app = express();
dotenv.config();

// Middlewares
app.use(bodyParser.json());
// Middleware para procesar texto plano
app.use(express.text());
app.use(morgan('dev'));
app.use(cors());

// Auth middleware
app.use(authMiddleware);

app.use('/api',billsRoutes);
app.use('/helpers',helpersRoutes);
app.use('/bot',botRoutes);
app.use('/dev',devRoutes);

// app.use('/google',googleRoutes)
// Routes
app.get('/', async (req, res) => {

  if(!req.apiKey) {
    return res.status(500).json({ error: 'Error al autenticar la solicitud' });
  }


  const emailDate =  "2025-05-13T15:57:46.000Z"

  const date = moment(emailDate).tz('America/Santiago').format('YYYY-MM-DDTHH:mm:ssZ');
  console.log("date", date);
  res.json({date});
  return

  // const communityResponse =  findDeliveryDayByComuna("vitacura","2025-05-17T14:00:01Z");
  // console.log("communityResponse", communityResponse);
  // res.json(communityResponse);

  return 

  if(!req.apiKey) {
    return res.status(500).json({ error: 'Error al autenticar la solicitud' });
  }

  // const allCoins = await getAllCoinsId(req.apiKey)
  // const allPayments = await getAllPaymentsConditions(req.apiKey)
  // const shops = await getShops(req.apiKey)
  // const priceList = await getPriceList(req.apiKey)
  // const getPriceListDetail = await getPriceListDetails(req.apiKey)
  // const documentAnalysis = await getDocumentAnalysis(req.apiKey)
  // const getClientss = await getClients(req.apiKey) // api/Sale/GetClients
  // const getClientFromId = await getClientByFileId(req.apiKey,"76.322.465-1")
  const sellersIdd = await sellersId(req.apiKey) 
  // const getProdss = await getProds(req.apiKey)
  // const getStoragess = await getStorages(req.apiKey)
  // const businessAnalysiss = await businessAnalysis(req.apiKey);// api/Sale/GetDocumentAnalisys
  // const codess = allPayments.items.map((desc) => desc.code);
  res.json({sellersIdd});
  return 
  const businessCenter =  await businessCenters(req.apiKey) //preguntar por API //api/Accounting/GetBusinessCenterPlan
  const saleBusinessCenterAccounts = {
    defaultSale : "EMPNEGVTAVTA000",
    CNPSale : "EMPNEGVTACCP000"
  }
  const classifierAnalysiss =  await classifierAnalysis(req.apiKey,saleBusinessCenterAccounts.defaultSale) //preguntar por API //api/Accounting/GetBusinessCenterPlan
  // const paymentConditionss = await  paymentConditions(req.apiKey); // api/Sale/GetPaymentConditions
  
  res.json(businessCenter)
  return
  // businessCenterLogic
  const {centrosNegocios} = businessCenter;

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




// Start server
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

