import { application } from "express";

const getAllCoinsId = async (apiKey) => {
    const coinsURL = `https://replapi.defontana.com/api/Sale/GetAllCoinsId?itemsPerPage=100&pageNumber=1`;
    const moneyTypes = await fetch(coinsURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const moneyTypesData = await moneyTypes.json();
    return moneyTypesData;
}

const getAllPaymentsConditions = async (apiKey) => {

    const paymentsConditionsURL = `https://replapi.defontana.com/api/Sale/GetPaymentConditions`
    const options = {
        method: 'GET',
        headers : { 
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    }

    const response = await fetch(paymentsConditionsURL, options);
    const paymentsConditions = await response.json();
    return paymentsConditions;

}


const getShops  = async (apiKey) => {
    const shopsURL = `https://replapi.defontana.com/api/Sale/GetShops?itemsPerPage=100&pageNumber=1`;
    const shops = await fetch(shopsURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const shopsData = await shops.json();
    return shopsData;
}

const getPriceList = async (apiKey) => {
    const priceListURL = `https://replapi.defontana.com/api/Sale/GetPriceList?itemsPerPage=100&pageNumber=1`;
    const priceList = await fetch(priceListURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const priceListData = await priceList.json();
    return priceListData;
}

const getPriceListDetails = async (apiKey) => {
    const priceListDetailsURL = `https://replapi.defontana.com/api/Sale/GetPriceListDetail?PriceListID=1&itemsPerPage=100&pageNumber=1`;
    const priceListDetails = await fetch(priceListDetailsURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const priceListDetailsData = await priceListDetails.json();
    return priceListDetailsData;
}

const getDocumentAnalysis = async (apiKey) => {
    const documentAnalysisURL = `https://replapi.defontana.com/api/Sale/GetDocumentAnalysis?itemsPerPage=100&pageNumber=1`;
    const documentAnalysis = await fetch(documentAnalysisURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const documentAnalysisData = await documentAnalysis.json();
    return documentAnalysisData;
}

const getClients = async(apiKey) =>{
    const clientsURL = `https://api.defontana.com/api/Sale/GetClients?status=1&itemsPerPage=200&pageNumber=2`;
    const clients = await fetch(clientsURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const clientsData = await clients.json();
    console.log("clientsData", clientsData);
    return clientsData;
}

const getClientByFileId= async(apiKey,fileId) =>{
    const clientURL = `https://api.defontana.com/api/Sale/GetClientsByFileID?fileId=${fileId}&status=1&itemsPerPage=10&pageNumber=1`;
    const client = await fetch(clientURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const clientData = await client.json();

    if(!clientData.success){
        return {
            success:false,
            message:clientData.message || "No existe el cliente"
        }
    }
    if(clientData.totalItems == 0){
        return {
            success:false,
            message:"No existe el cliente"
        }
    }

    const clientInfo = {
        city: clientData.clientList[0].city,
        legalCode: clientData.clientList[0].legalCode,
        adress: clientData.clientList[0].adress,
        district: clientData.clientList[0].district,
        email: clientData.clientList[0].email,
        state: clientData.clientList[0].state,
        business: clientData.clientList[0].business,
        companyId: clientData.clientList[0].companyID,
        fileID: clientData.clientList[0].fileID,
        localId: clientData.clientList[0].localID,
        coinID: clientData.clientList[0].coinID,
        paymentID: clientData.clientList[0].paymentID,
        name: clientData.clientList[0].name,
        phone: clientData.clientList[0].phone, 
    }
    // const {
    //     city,
    //     legalCode,
    //     adress,
    //     district,
    //     email,
    //     state,
    //     business,
    //     companyId,
    //     fileID,
    //     localId,
    //     coinID,
    //     paymentID,
    //     name,
    //     phone
    // } = clientData.clientList[0];

    return clientInfo;
}


const sellersId = async(apiKey) =>{
    const sellersURL = `https://replapi.defontana.com/api/sale/GetSellers?itemsPerPage=100&pageNumber=1`;
    const sellers = await fetch(sellersURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const sellersData = await sellers.json();
    return sellersData;
}


const getProds = async(apiKey) =>{
    const prodsURL = `https://replapi.defontana.com/api/Sale/Getproducts?status=0&itemsPerPage=200&pageNumber=1`;
    const prods = await fetch(prodsURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const prodsData = await prods.json();
    return prodsData;
}

const getStorages = async (apiKey) => {
    const storagesURL = `https://replapi.defontana.com/api/Sale/GetStorages?itemsPerPage=100&pageNumber=1`;
    const storages = await fetch(storagesURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const storagesData = await storages.json();
    return storagesData;
}

const businessAnalysis = async (apiKey) => {
    const businessAnalysisURL = `https://replapi.defontana.com/api/Sale/GetDocumentAnalysis?itemsPerPage=100&pageNumber=1`;
    const businessAnalysis = await fetch(businessAnalysisURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const businessAnalysisData = await businessAnalysis.json();
    return businessAnalysisData;
}

const paymentConditions = async (apiKey) => {
    const paymentConditionsURL = `https://replapi.defontana.com/api/Sale/GetPaymentConditions`;
    const paymentConditions = await fetch(paymentConditionsURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const paymentConditionsData = await paymentConditions.json();
    return paymentConditionsData;
}

const businessCenters = async (apiKey) => {
    const businessCentersURL = `https://replapi.defontana.com/api/Accounting/GetBusinessCenterPlan`;
    const businessCenters = await fetch(businessCentersURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const businessCentersData = await businessCenters.json();


    const {centrosNegocios} = businessCentersData;
    

    const expectedBusinessCenter = centrosNegocios.find(desc => desc.code === "EMP000000000000")
    ?.descendientes.find((desc) => {
      return desc.code === "EMPNEG000000000";
    })
    ?.descendientes.find((desc) => {
      return desc.code === "EMPNEGVTA000000";
    })?.descendientes
  


    return expectedBusinessCenter;
}

const classifierAnalysis = async (apiKey,account) => {
    const classifierAnalysisURL = `https://replapi.defontana.com/api/Accounting/GetAccountAnalisys?Account=EMPNEGVTAVTA000`;
    const classifierAnalysis = await fetch(classifierAnalysisURL, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${apiKey}`
        }
    });
    const classifierAnalysisData = await classifierAnalysis.json();
    return classifierAnalysisData;
}


export { getAllCoinsId ,
    getAllPaymentsConditions,
    getShops,
    getPriceList,
    getPriceListDetails,
    getDocumentAnalysis,
    getClients,
    getClientByFileId,
    sellersId,
    getProds,
    getStorages,
    businessAnalysis,
    paymentConditions,
    businessCenters,
    classifierAnalysis
};