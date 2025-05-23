import moment from 'moment';

class Bill {
    constructor({
        apiKey = null,
        documentType = null,
        firstFolio = null,
        lastFolio = null,
        clientFile = null,
        paymentCondition = null,
        sellerFileId = null,
        businessCenter = null,
        shopId = null,
        priceList = null,
        giro = null,
        attachedDocuments = null,
        storage = null,
        details = null,
        ventaRecDesGlobal = null,
        gloss = null,
        customFields = null,
        isTransferDocument = null,
        deliveryDay = null
    } = {}) {
        this.apiKey = apiKey;
        this.documentType = documentType;
        this.firstFolio = firstFolio;
        this.lastFolio = lastFolio;
        this.clientFile = clientFile;
        this.paymentCondition = paymentCondition;
        this.sellerFileId = sellerFileId;
        this.businessCenter = businessCenter;
        this.shopId = shopId;
        this.priceList = priceList;
        this.giro = giro;
        this.attachedDocuments = attachedDocuments;
        this.storage = storage;
        this.details = details;
        this.ventaRecDesGlobal = ventaRecDesGlobal;
        this.gloss = gloss;
        this.customFields = customFields;
        this.isTransferDocument = isTransferDocument;
        this.deliveryDay = deliveryDay;
    }

    validate() {
        if (!this.apiKey || this.apiKey === "") {
            throw { code: 400, error: "Bad request", message: 'API key is required' };
        }
        if (!this.documentType || this.documentType === "") {
            throw { code: 400, error: "Bad request", message: 'Document type is required' };
        }
        if ((this.firstFolio == null || this.firstFolio == undefined) || this.firstFolio === "") {
            throw { code: 400, error: "Bad request", message: 'First folio is required' };
        }
        if ((this.lastFolio == null || this.lastFolio === undefined) || this.lastFolio === "") {
            throw { code: 400, error: "Bad request", message: 'Last folio is required' };
        }
        if (!this.clientFile || this.clientFile === "") {
            throw { code: 400, error: "Bad request", message: 'Client file is required' };
        }
        if (!this.paymentCondition || this.paymentCondition === "") {
            throw { code: 400, error: "Bad request", message: 'Payment condition is required' };
        }
        if (!this.sellerFileId || this.sellerFileId === "") {
            throw { code: 400, error: "Bad request", message: 'Seller file ID is required' };
        }
        if (!this.businessCenter || this.businessCenter === "") {
            throw { code: 400, error: "Bad request", message: 'Business center is required' };
        }
        if (!this.shopId || this.shopId === "") {
            throw { code: 400, error: "Bad request", message: 'Shop ID is required' };
        }
        if (!this.priceList || this.priceList === "") {
            throw { code: 400, error: "Bad request", message: 'Price list is required' };
        }
        if (!this.giro || this.giro === "") {
            throw { code: 400, error: "Bad request", message: 'Giro is required' };
        }
        if (!this.attachedDocuments || !Array.isArray(this.attachedDocuments)) {
            throw { code: 400, error: "Bad request", message: 'Attached documents is required and must be an array' };
        }
        // Removed validation for district and city as they are not part of the class properties
        if (!this.storage || this.storage === "") {
            throw { code: 400, error: "Bad request", message: 'storage is required' };
        }
        if (!this.details || !Array.isArray(this.details)) {
            throw { code: 400, error: "Bad request", message: 'Details is required and must be an array' };
        }
        if (!this.ventaRecDesGlobal || !Array.isArray(this.ventaRecDesGlobal)) {
            throw { code: 400, error: "Bad request", message: 'Global discount is required and must be an array' };
        }
        // gloss is a free input, so no validation is needed
        if (!this.customFields || !Array.isArray(this.customFields)) {
            throw { code: 400, error: "Bad request", message: 'Custom fields is required and must be an array' };
        }
        if (typeof this.isTransferDocument !== 'boolean') {
            throw { code: 400, error: "Bad request", message: 'isTransferDocument is required' };
        }
    }

    async getFileid() {
        const response = await this.#getClientByFileId(this.apiKey, this.clientFile);
        console.log(response);
        return response;
    }

    async toJSON() {
        try {
            const clientData = await this.#getClientByFileId(this.apiKey, this.clientFile);
            if (!clientData.success) {
                throw { code: 400, error: "Not Found", message: clientData.message };
            }

            const paymentCondition = this.#checkpaymentCondition();
            if (!paymentCondition.success) {
                throw { code: 400, error: "Bad Request", message: paymentCondition.message };
            }

            // const clientAnalysis = this.#getClientAnalysis();

            const sellerFileId = await this.#getSellerInfo();
            if (!sellerFileId.success) {
                throw { code: 400, error: "Not Found", message: sellerFileId.message };
            }

            const shopData = await this.#getShops();
            if (!shopData.success) {
                throw { code: 400, error: "Not Found", message: shopData.message };
            }

            const priceListData = await this.#getPriceList();
            if (!priceListData.success) {
                throw { code: 400, error: "Not Found", message: priceListData.message };
            }

            const attachedDocuments = this.#getAttachedDocuments();
            console.log(attachedDocuments);
            if (!attachedDocuments.success) {
                throw { code: 400, error: "Not Found", message: attachedDocuments.message };
            }

            const storageData = await this.#getStorage();
            if (!storageData.success) {
                throw { code: 400, error: "Not Found", message: storageData.message };
            }
            const businessAnalysis = await this.#getBusinessAnalysis();
            console.log(businessAnalysis);
            if (!businessAnalysis.success) {
                throw { code: 400, error: "Not Found", message: businessAnalysis.message };
            }

            const detailsList = await this.#getDetailsList(businessAnalysis.businessAnalysis.saleAnalysis);
            if (!detailsList.success) {
                throw { code: 400, error: "Not Found", message: detailsList.message };
            }

            const saleTaxes = await this.#getSaleTaxes(businessAnalysis.businessAnalysis.taxeAnalysis);
            const globalDiscount = this.#getGlobalDiscount();
            const uniqueCode = moment().format("YYYYMMDDHHmmss");

            let total = 0
            this.details.forEach((detail) => {
                let sum = detail.price * detail.quantity;
                total += sum;
            })

            return {
                documentType: this.documentType,
                firstFolio: this.firstFolio,
                lastFolio: this.lastFolio,
                externalDocumentID: uniqueCode,
                emissionDate: this.#getEmissionDate(),
                firstFeePaid: this.#getFirstFeePaid(paymentCondition.paymentDays),
                clientFile: clientData.fileID,
                contactIndex: clientData.address,
                rutMandante: "",
                paymentCondition: paymentCondition.success ? paymentCondition.paymentMethod : "",
                sellerFileId: sellerFileId.code,
                clientAnalysis: businessAnalysis.businessAnalysis.clientAnalysis,
                billingCoin: "PESO",
                billingRate: 1,
                shopId: shopData.code,
                priceList: `${priceListData.priceListID}`,
                giro: this.giro,
                district: clientData.district,
                city: clientData.city,
                contact: -1,
                attachedDocuments: attachedDocuments.attachedDocuments,
                storage: storageData.storage,
                details: detailsList.detailList,
                saleTaxes: saleTaxes,
                ventaRecDesGlobal: globalDiscount,
                gloss: this.gloss,
                customFields: [],
                isTransferDocument: true
            };
            // isTransferDocument: total >= 1600000 ? true : false,
        } catch (error) {
            if (error.code && error.message) {
                throw error;
            }
            // throw { code: 500, error: "Internal Server Error", message: "An unexpected error occurred" };
            throw { code: 500, error: error, message: "An unexpected error occurred" };
        }
    }

    #checkpaymentCondition() {
        try {
            const paymentCodes = [

                { code: "3DIAS", name: ["3DIAS", "3"], paymentDays: 3 },
                { code: "ANTICIPADO", name: ["ANTICIPADO", "0"], paymentDays: 1 },
                { code: "CONTADO", name: ["CONTADO", "1"], paymentDays: 1 },
                { code: "CREDITO120", name: ["CREDITO120", "120"], paymentDays: 120 },
                { code: "CREDITO10", name: ["CREDITO10", "10"], paymentDays: 10 },
                { code: "CREDITO7", name: ["CREDITO7", "7"], paymentDays: 7 },
                { code: "CREDITO15", name: ["CREDITO15", "15"], paymentDays: 15 },
                { code: "CREDITO30", name: ["CREDITO30", "30"], paymentDays: 30 },
                { code: "CREDITO45", name: ["CREDITO45", "45"], paymentDays: 45 },
                { code: "CREDITO3060", name: ["CREDITO3060", "360"], paymentDays: 30 },
                { code: "CREDITO306090", name: ["CREDITO306090", "3690"], paymentDays: 30 },
                { code: "CHEQUE", name: ["CHEQUE", "C1"], paymentDays: 1 },
                { code: "CHEQUE15DIAS", name: ["CHEQUE15DIAS", "C15"], paymentDays: 15 },
                { code: "CHEQUE30IAS", name: ["CHEQUE30IAS", "C30"], paymentDays: 30 },
                { code: "CREDITO5", name: ["CREDITO5", "5"], paymentDays: 5 },
                { code: "CREDITO60", name: ["CREDITO60", "60"], paymentDays: 60 },
                { code: "CREDITO90", name: ["CREDITO90", "90"], paymentDays: 90 }
            ];
            const paymentData = paymentCodes.find(payment => payment.name.includes(this.paymentCondition));
            if (!paymentData) {
                return {
                    success: false,
                    message: "Invalid payment method"
                };
            }
            return {
                success: true,
                paymentDays: paymentData.paymentDays,
                paymentMethod: paymentData.code
            };
        } catch (error) {
            return {
                success: false,
                message: "Failed to check payment condition, please verify the payment method"
            };
        }
    }

    #getClientAnalysis() {
        return {
            "accountNumber": "1110401001",
            "businessCenter": "",
            "classifier01": "",
            "classifier02": ""
        }
    }

    #getSellerInfo = async () => {
        try {

            const sellersURL = `${process.env.SALE_API_URL}GetSellers?itemsPerPage=100&pageNumber=1`;
            const sellers = await fetch(sellersURL, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`
                }
            });

            // check if response is ok
            if (!sellers.ok) {
                return {
                    success: false,
                    message: "Failed to fetch seller information"
                };
            }

            const sellersData = await sellers.json();

            if (!sellersData.success || sellersData.totalItems === 0) {
                return {
                    success: false,
                    message: sellersData.message || "No sellers available"
                }
            }

            const sellerInfo = sellersData.sellerList.find(seller => seller.code === this.sellerFileId);

            if (!sellerInfo) {
                return {
                    success: false,
                    message: "Seller not found"
                }
            }
            console.log({ sellerInfo });
            const sellerResponse = {
                code: sellerInfo.code,
                mail: sellerInfo.mail,
                name: sellerInfo.name,
                success: true
            }
            console.log({ sellerResponse });

            return sellerResponse;
        } catch (error) {
            return {
                success: false,
                message: "Failed to fetch seller information"
            };
        }
    }

    /**
     * Fetches client information by file ID from an external API.
     * 
     * @private
     * @async
     * @param {string} apiKey - The API key used for authorization.
     * @param {string} fileId - The file ID to fetch the client information for.
     * @returns {Promise<Object>} A promise that resolves to an object containing client information or an error message.
     * 
     * @property {boolean} success - Indicates whether the operation was successful.
     * @property {string} [message] - An error message if the operation was not successful.
     * @property {string} [city] - The city of the client.
     * @property {string} [legalCode] - The legal code of the client.
     * @property {string} [adress] - The address of the client.
     * @property {string} [district] - The district of the client.
     * @property {string} [email] - The email of the client.
     * @property {string} [state] - The state of the client.
     * @property {string} [business] - The business name of the client.
     * @property {string} [companyId] - The company ID of the client.
     * @property {string} [fileID] - The file ID of the client.
     * @property {string} [localId] - The local ID of the client.
     * @property {string} [coinID] - The coin ID associated with the client.
     * @property {string} [paymentID] - The payment ID associated with the client.
     * @property {string} [name] - The name of the client.
     * @property {string} [phone] - The phone number of the client.
     */
    #getClientByFileId = async (apiKey, fileId) => {
        try {
            const clientURL = `${process.env.SALE_API_URL}GetClientsByFileID?fileId=${fileId}&status=1&itemsPerPage=10&pageNumber=1`;
            const client = await fetch(clientURL, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${apiKey}`
                }
            });
            const clientData = await client.json();

            if (!clientData.success || clientData.totalItems === 0) {
                return {
                    success: false,
                    message: clientData.message || "Client not found"
                };
            }

            if (clientData.clientList[0].active !== "S") {
                return {
                    success: false,
                    message: "Client is inactive"
                }
            }
            console.log(clientData.clientList);
            return {
                city: clientData.clientList[0].city,
                legalCode: clientData.clientList[0].legalCode,
                address: clientData.clientList[0].address,
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
                success: true
            };
        } catch (error) {
            return {
                success: false,
                message: "Failed to fetch client data, invalid client ID"
            };
        }
    }

    #getEmissionDate() {

        const deliveryDay = this.deliveryDay ? moment(this.deliveryDay, "YYYY-MM-DD") : null;

        if (deliveryDay && deliveryDay.isValid()) {
            return {
                day: deliveryDay.format('D'),
                month: deliveryDay.format('M'),
                year: deliveryDay.format('YYYY')
            }
        }
        // If deliveryDay is not provided or invalid, use today's date
        const today = moment();

        return {
            day: today.format('D'),
            month: today.format('M'),
            year: today.format('YYYY')
        }
    }

    #getFirstFeePaid(paymentsDays) {

        const deloveryDay = this.deliveryDay ? moment(this.deliveryDay, "YYYY-MM-DD") : null;

        if (deloveryDay && deloveryDay.isValid()) {
            const paymentDate = deloveryDay.add(paymentsDays, 'days');
            return {
                day: paymentDate.format('D'),
                month: paymentDate.format('M'),
                year: paymentDate.format('YYYY')
            }
        }

        const paymentDate = moment().add(paymentsDays, 'days');
        return {
            day: paymentDate.format('D'),
            month: paymentDate.format('M'),
            year: paymentDate.format('YYYY')
        }
    }

    #getShops = async () => {
        try {
            const shopsURL = `${process.env.SALE_API_URL}GetShops?itemsPerPage=10&pageNumber=1`;
            const shops = await fetch(shopsURL, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`
                }
            });

            const shopsData = await shops.json();

            if (!shopsData.success || shopsData.totalItems === 0) {
                return {
                    success: false,
                    message: shopsData.message || "No shops available"
                };
            }

            const shopInfo = shopsData.shopList.find(shop => shop.code === this.shopId);

            if (!shopInfo) {
                return {
                    success: false,
                    message: "Shop not found"
                };
            }

            return { ...shopInfo, success: true };
        } catch (error) {
            return {
                success: false,
                message: "Failed to fetch shop data"
            };
        }
    }

    #getPriceList = async () => {
        try {
            const priceListURL = `${process.env.SALE_API_URL}GetPriceList?itemsPerPage=100&pageNumber=1`;
            const priceList = await fetch(priceListURL, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`
                }
            });
            const priceListData = await priceList.json();

            if (!priceListData.success || priceListData.totalItems === 0) {
                return {
                    success: false,
                    message: priceListData.message || "No price lists available"
                };
            }

            const priceListInfo = priceListData.priceList.find(price => price.priceListID == this.priceList);
            if (!priceListInfo) {
                return {
                    success: false,
                    message: "Price list not found"
                };
            }

            return { ...priceListInfo, success: true };
        } catch (error) {
            return {
                success: false,
                message: "Failed to fetch price list data"
            };
        }
    }

    #getAttachedDocuments = () => {
        try {
            // this.#validateAttachedDocuments(document);
            if (this.attachedDocuments.length === 0) {
                return {
                    success: true,
                    attachedDocuments: []
                };
            }

            const attachedDocuments = this.attachedDocuments;

            let attachedDocumentsList = [];
            attachedDocuments.forEach((document) => {
                const date = document.date != "" ? document.date : moment().format("DD-MM-YYYY");
                const day = moment(date, "DD-MM-YYYY").format("D");
                const month = moment(date, "DD-MM-YYYY").format("M");
                const year = moment(date, "DD-MM-YYYY").format("YYYY");
                const documentInfo = {
                    "date": {
                        "day": day,
                        "month": month,
                        "year": year
                    },
                    "documentTypeId": `${document.documentType}`,
                    "folio": `${document.folio}`,
                    "reason": ""
                }
                attachedDocumentsList.push(documentInfo);
            });

            const attachedDocumentsResponse = {
                "attachedDocuments": attachedDocumentsList,
                success: true
            }

            return attachedDocumentsResponse;

        } catch (error) {
            console.log(error)
            return {
                success: false,
                message: "Failed to fetch attached documents"
            };
        }
    }


    #getStorage = async () => {
        try {
            const storageURL = `${process.env.SALE_API_URL}GetStorages?itemsPerPage=100&pageNumber=1`;
            const storage = await fetch(storageURL, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`
                }
            });

            const storageData = await storage.json();

            if (!storageData.success || storageData.totalItems === 0) {
                return {
                    success: false,
                    message: storageData.message || "No storage available"
                };
            }

            const storageInfo = storageData.storageList.find(storage => storage.code === this.storage);

            if (!storageInfo) {
                return {
                    success: false,
                    message: "Storage not found"
                };
            }

            const storageResponse = {
                "code": storageInfo.code,
                "description": storageInfo.description,
                "saleAvailable": storageInfo.saleAvailable,
                "active": storageInfo.active,
            }

            console.log({ storageResponse });

            if (storageResponse.active != "S") {
                return {
                    success: false,
                    message: "Storage is inactive"
                }
            }

            storageResponse.success = true;
            // console.log({storageResponse});

            const response = {
                "code": "",
                "motive": "",
                "storageAnalysis": {
                    "accountNumber": "",
                    "businessCenter": "",
                    "classifier01": "",
                    "classifier02": ""
                }
            }
            return { success: true, storage: response }
            // return storageResponse;
        } catch (error) {
            return {
                success: false,
                message: "Failed to fetch storage data"
            };
        }
    }
    #getBusinessAnalysis = async () => {

        try {
            let skip = false;
            let paginationInfo = this.#getPagintation();
            let data;
            let { currentPageNumber, totalItems, totalPossibleIterations, itemsPerPage } = paginationInfo;
            while (!skip) {
                const businessAnalysisURL = `${process.env.SALE_API_URL}GetDocumentAnalysis?itemsPerPage=${itemsPerPage}&pageNumber=${currentPageNumber}`;
                const businessAnalysis = await fetch(businessAnalysisURL, {
                    method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${this.apiKey}`
                    }
                });
                const businessAnalysisData = await businessAnalysis.json();
                if (businessAnalysisData.totalItems === 0) {
                    console.log({ "NOTpass": "NOTpass" });
                    return {
                        success: false,
                        message: businessAnalysisData.message || "No business analysis available"
                    };
                }

                //get total items and total possible iterations
                totalItems = businessAnalysisData.totalItems;
                totalPossibleIterations = Math.ceil(businessAnalysisData.totalItems / itemsPerPage);
                const businessAnalysisInfo = businessAnalysisData.documentList.find((analysis) => {
                    return analysis.documentId === this.documentType
                });

                if (businessAnalysisInfo) {
                    data = businessAnalysisInfo;
                    skip = true;
                }

                currentPageNumber++;


                if (totalPossibleIterations < currentPageNumber) {
                    skip = true;
                }

            }

            if (!data) {
                return {
                    success: false,
                    message: "Business analysis not found"
                };
            }
            const { analysisDetail } = data;
            const clientAnalysis = analysisDetail.find(analysis => analysis.analysisType === "CLT");
            const taxeAnalysis = analysisDetail.find(analysis => analysis.analysisType === "IMP");
            const saleAnalysis = analysisDetail.find(analysis => analysis.analysisType === "VTA");

            if (!clientAnalysis || !taxeAnalysis || !saleAnalysis) {
                return {
                    success: false,
                    message: "Client, sale or tax analysis not found"
                };
            }
            const businessCenterCode = this.#getBusinessCenterCode(this.businessCenter);

            if (!businessCenterCode.success) {
                return {
                    success: false,
                    message: businessCenterCode.message || "Business center not found"
                };
            }
            const saleBusinessCenter = await this.#getSaleBusinessCenterAccounts(businessCenterCode.code);
            console.log("esto falla por la cara")
            console.log(saleBusinessCenter);
            if (!saleBusinessCenter.success) {
                console.log({ "NOTpass": "NOTpass" });
                console.log(saleBusinessCenter);
                return {
                    success: false,
                    message: saleBusinessCenter.message || "Sale business center not found"
                };
            }
            const businessAnalysis = {
                "clientAnalysis": {
                    "accountNumber": clientAnalysis.accountNumber,
                    "businessCenter": "",
                    "classifier01": "",
                    "classifier02": ""
                },
                "saleAnalysis": {
                    "accountNumber": saleAnalysis.accountNumber,
                    "businessCenter": `${businessCenterCode.code}`,
                    "classifier01": "",
                    "classifier02": ""
                },
                "taxeAnalysis": {
                    "accountNumber": taxeAnalysis.accountNumber,
                    "businessCenter": "",
                    "classifier01": "",
                    "classifier02": ""
                }
            }

            return { businessAnalysis, success: true };
        } catch (error) {
            return {
                success: false,
                // message: "Failed to fetch business analysis data"
                message: error.message || "Failed to fetch business analysis data"
            };
        }
    }
    #getBusinessCenterCode = (businessCenterName) => {
        try {

            //CHECK IF businessCenterName IS a valid string
            if (typeof businessCenterName !== 'string' || businessCenterName.trim() === '') {
                return {
                    success: false,
                    message: "Invalid business center name"
                };
            }
            const saleAccounts_businessCenter = [
                {
                    code: "EMPNEGVTAVTA000",
                    desc: ["VNT", "VENTA", "VENTAS"],
                },
                {
                    code: "EMPNEGVTACCP000",
                    desc: ["CNP", "CONCEPCION"],
                }
            ];
            businessCenterName = businessCenterName.trim().toUpperCase();
            const foundBusinessCenter = saleAccounts_businessCenter.find((desc) => {
                return desc.desc.includes(businessCenterName);
            });

            if (!foundBusinessCenter) {
                return {
                    success: false,
                    message: `Business center: ${businessCenterName} not found`

                };
            }

            return {
                code: foundBusinessCenter.code,
                success: true
            }
        } catch (error) {
            console.log(error)
            return {
                success: false,
                message: `Failed to fetch business center code using ${businessCenterName}`
            };
        }
    }
    #getDetailsList = async (saleAnalysis) => {
        try {
            const details = this.details;
            if (details.length === 0) {
                return {
                    success: false,
                    message: "No details available"
                };
            }
            //validar elementos del array proporcionado son correctos
            const invalidDetail = details.find(detail => {
                return !detail.code || typeof detail.code !== "string" ||
                    detail.quantity === undefined || !Number.isFinite(detail.quantity);
            });
            if (invalidDetail) {
                return {
                    success: false,
                    message: "Each detail must have a valid 'code' (string) and 'quantity' (integer)"
                };
            }
            //obtener todos los productos asociados a la empresa
            const products = await this.#getProducts();
            if (!products.success) {
                return {
                    success: false,
                    message: products.message
                };
            }
            const { productList } = products;
            const find70724043633542 = productList.find(product => product.code === "70724043633542");
            console.log("PINKKKKKKKKKKKKKKKKKKKKKK", find70724043633542);
            //verificar que los productos de los detalles existen en la lista de productos
            const productCodes = productList.map(product => product.code);
            let detailList = [];

            console.log({ details });
            details.forEach((detail) => {

                if (detail.quantity <= 0) {
                    return;
                }

                const productInfo = productList.find(product => product.code == detail.code);
                console.log({ productInfo });
                if (!productInfo) {
                    return {
                        success: false,
                        message: `Product with code ${detail.code} not found in the product list`
                    };
                }

                const price = detail.price != 0 ? detail.price : productInfo.sellPrice;

                detailList.push({
                    "type": "A",
                    "isExempt": false,
                    "code": productInfo.code,
                    "count": detail.quantity,
                    "productName": productInfo.name,
                    "productNameBarCode": "",
                    "price": price,
                    "discount": {
                        "type": 0,
                        "value": 0
                    },
                    "unit": productInfo.unit,  // UNIDAD DEL PRODUCTO
                    "analysis": saleAnalysis,
                    "useBatch": false,  // EN CASO DE USAR LOTES, SE ENVÍA COMO TRUE, SINO FALSE
                    "batchInfo": []
                });

            })

            return {
                success: true,
                detailList: detailList
            };
        } catch (error) {
            console.log(error)
            return {
                success: false,
                message: "Failed to fetch document details"
            };
        }
    }

    #getProducts = async () => {
        try {
            let paginationInfo = this.#getPagintation();
            const productsURL = `${process.env.SALE_API_URL}Getproducts?status=0&itemsPerPage=100&pageNumber=1`;
            const prods = await fetch(productsURL, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`
                }
            });

            const productsData = await prods.json();

            if (!productsData.success || productsData.totalItems === 0) {
                return {
                    success: false,
                    message: productsData.message || "No products available"
                };
            }

            //get total items and total possible iterations
            paginationInfo.totalItems = productsData.totalItems;
            paginationInfo.totalPossibleIterations = Math.ceil(productsData.totalItems / paginationInfo.itemsPerPage);

            let allProducts = productsData.productList;

            if (paginationInfo.totalPossibleIterations > 1) {
                for (let i = 2; i <= paginationInfo.totalPossibleIterations; i++) {
                    const productsURL = `${process.env.SALE_API_URL}Getproducts?status=0&itemsPerPage=10&pageNumber=${i}`;
                    const prods = await fetch(productsURL, {
                        method: 'GET',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': `Bearer ${this.apiKey}`
                        }
                    });

                    const productsData = await prods.json();

                    if (!productsData.success || productsData.totalItems === 0) {
                        return {
                            success: false,
                            message: productsData.message || "No products available"
                        };
                    }
                    allProducts = [...allProducts, ...productsData.productList];
                }
            }

            return { success: true, productList: allProducts };
        } catch (error) {
            return {
                success: false,
                message: "Failed to fetch product data"
            };
        }
    }

    #getSaleTaxes = async (taxeAnalysis) => {
        try {
            return [
                {
                    code: "IVA",
                    value: 19,
                    taxeAnalysis: taxeAnalysis
                }
            ];
        } catch (error) {
            return {
                success: false,
                message: "Failed to fetch sale taxes"
            };
        }
    }

    #getGlobalDiscount() {
        try {
            return [

            ];
        } catch (error) {
            return {
                success: false,
                message: "Failed to fetch global discount"
            };
        }
    }

    #accountingInfo() {

        return {
            "businessCentersData": {
                "success": true,
                "message": "Centros de negocio obtenidos correctamente",
                "exceptionMessage": null,
                "centrosNegocios": [
                    {
                        "code": "EMP000000000000",
                        "description": "EMPRESA",
                        "imputable": "N",
                        "activo": "S",
                        "descendientes": [
                            {
                                "code": "EMPGES000000000",
                                "description": "AREAS DE GESTION",
                                "imputable": "N",
                                "activo": "S",
                                "descendientes": [
                                    {
                                        "code": "EMPGESGES000000",
                                        "description": "AREAS DE GESTION",
                                        "imputable": "N",
                                        "activo": "S",
                                        "descendientes": [
                                            {
                                                "code": "EMPGESGESADM000",
                                                "description": "ADMINISTRACION",
                                                "imputable": "S",
                                                "activo": "S",
                                                "descendientes": null
                                            },
                                            {
                                                "code": "EMPGESGESGER000",
                                                "description": "GERENCIA",
                                                "imputable": "S",
                                                "activo": "S",
                                                "descendientes": null
                                            },
                                            {
                                                "code": "EMPGESGESLOG000",
                                                "description": "LOGISTICA",
                                                "imputable": "S",
                                                "activo": "S",
                                                "descendientes": null
                                            },
                                            {
                                                "code": "EMPGESGESOPE000",
                                                "description": "OPERACIONES",
                                                "imputable": "S",
                                                "activo": "S",
                                                "descendientes": null
                                            }
                                        ]
                                    }
                                ]
                            },
                            {
                                "code": "EMPNEG000000000",
                                "description": "AREAS DE NEGOCIOS",
                                "imputable": "N",
                                "activo": "S",
                                "descendientes": [
                                    {
                                        "code": "EMPNEGPRO000000",
                                        "description": "PROYECTOS",
                                        "imputable": "N",
                                        "activo": "S",
                                        "descendientes": [
                                            {
                                                "code": "EMPNEGPRO001000",
                                                "description": "PROYECTO ESPECIFICO 1",
                                                "imputable": "S",
                                                "activo": "S",
                                                "descendientes": null
                                            },
                                            {
                                                "code": "EMPNEGPRO002000",
                                                "description": "PROYECTO ESPECIFICO 2",
                                                "imputable": "S",
                                                "activo": "S",
                                                "descendientes": null
                                            }
                                        ]
                                    },
                                    {
                                        "code": "EMPNEGSER000000",
                                        "description": "SERVICIOS",
                                        "imputable": "N",
                                        "activo": "S",
                                        "descendientes": [
                                            {
                                                "code": "EMPNEGSERSER000",
                                                "description": "SERVICIOS",
                                                "imputable": "S",
                                                "activo": "S",
                                                "descendientes": null
                                            }
                                        ]
                                    },
                                    {
                                        "code": "EMPNEGVTA000000",
                                        "description": "VENTAS",
                                        "imputable": "N",
                                        "activo": "N",
                                        "descendientes": [
                                            {
                                                "code": "EMPNEGVTACCP000",
                                                "description": "CONCEPCION",
                                                "imputable": "S",
                                                "activo": "S",
                                                "descendientes": null
                                            },
                                            {
                                                "code": "EMPNEGVTAVTA000",
                                                "description": "VENTAS",
                                                "imputable": "S",
                                                "activo": "S",
                                                "descendientes": null
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        }
    }

    #getSaleBusinessCenterAccounts = async (businessCenterCode) => {
        try {
            // const businessCentersURL = `${process.env.ACCOUNTING_API_URL_PROD}GetBusinessCenterPlan`;
            // const businessCenters = await fetch(businessCentersURL, {
            //     method: 'GET',
            //     headers: {
            //         'Content-Type': 'application/json',
            //         'Authorization': `Bearer ${this.apiKey}`
            //     }
            // });
            const businessCentersData = this.#accountingInfo().businessCentersData;

            const { centrosNegocios } = businessCentersData;

            const allBusinessCenters = centrosNegocios.find(desc => desc.code === "EMP000000000000")
                ?.descendientes.find((desc) => {
                    return desc.code === "EMPNEG000000000";
                })
                ?.descendientes.find((desc) => {
                    return desc.code === "EMPNEGVTA000000";
                })?.descendientes

            if (!allBusinessCenters || allBusinessCenters.length === 0) {
                return {
                    success: false,
                    message: "Business center not found"
                };
            }

            const expectedBusinessCenter = allBusinessCenters.find((desc) => {
                return desc.code === businessCenterCode;
            });
            if (!expectedBusinessCenter) {
                return {
                    success: false,
                    message: `Business center: ${businessCenterCode} not found`
                };
            }

            if (expectedBusinessCenter.activo !== "S" || expectedBusinessCenter.imputable !== "S") {
                return {
                    success: false,
                    message: `Business center: ${businessCenterCode} is inactive or not imputable`
                };
            }


            const needClassifier = await this.#needClassifier(expectedBusinessCenter.code).classifierAnalysisData;
           
            if (!needClassifier.success) {
                
                return {
                    success: false,
                    message: needClassifier.message
                };
            }

            expectedBusinessCenter.success = true;
            return expectedBusinessCenter;

        } catch (error) {
            return {
                success: false,
                message: "Failed to fetch sale business center accounts"
            };
        }
    }

    #needClassifier = (businessCenterCode) => {
        if (businessCenterCode === "EMPNEGVTACCP000") {
            return {
                "classifierAnalysisData": {
                    "usesClassifier2Analysis": false,
                    "usesClassifier1Analysis": false,
                    "usesReferenceCurrencyAnalysis": false,
                    "usesDocumentAnalysis": false,
                    "usesFileAnalysis": false,
                    "usesBussinessCenterAnalysis": false,
                    "usesBankAnalysis": false,
                    "success": true,
                    "message": "",
                    "exceptionMessage": null
                }
            }

        }
        if (businessCenterCode === "EMPNEGVTAVTA000") {
            return {
                "classifierAnalysisData": {
                    "usesClassifier2Analysis": false,
                    "usesClassifier1Analysis": false,
                    "usesReferenceCurrencyAnalysis": false,
                    "usesDocumentAnalysis": false,
                    "usesFileAnalysis": false,
                    "usesBussinessCenterAnalysis": false,
                    "usesBankAnalysis": false,
                    "success": true,
                    "message": "",
                    "exceptionMessage": null
                }
            }
        }


        return { "classifierAnalysisData": {
            success: false,
        }}
    }


    #needClassifier_old = async (businessCenterCode) => {
        try {
            const classifierAnalysisURL = `${process.env.ACCOUNTING_API_URL_PROD}GetAccountAnalisys?Account=${businessCenterCode}`;
            const classifierAnalysis = await fetch(classifierAnalysisURL, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`
                }
            });
            const classifierAnalysisData = await classifierAnalysis.json();

            if (!classifierAnalysisData.success) {
                return {
                    success: false,
                    message: classifierAnalysisData.message || "No classifier analysis available"
                };
            }

            if (!classifierAnalysisData.usesClassifier2Analysis || !classifierAnalysisData.usesClassifier1Analysis) {
                return {
                    success: true,
                    message: "Classifier analysis available"
                };
            }

            if (classifierAnalysisData.usesClassifier2Analysis || classifierAnalysisData.usesClassifier1Analysis) {
                return {
                    success: false,
                    message: "Classifier analysis NOT available"
                };
            }
        } catch (error) {
            return {
                success: false,
                message: "Failed to fetch sale classifier"
            }
        }
    }


    // utils for pagination
    /**
     * Returns pagination information for the API response.
     * 
     * @private
     * @returns {Object} An object containing pagination information.
     * @property {number} currentPageNumber - The current page number.
     * @property {number} totalItems - The total number of items.
     * @property {number} totalPossibleIterations - The total number of possible iterations.
     * @property {number} itemsPerPage - The number of items per page.
     */

    #getPagintation = () => {
        return {
            currentPageNumber: 1,
            totalItems: 0,
            totalPossibleIterations: 1,
            itemsPerPage: 100
        }
    }

    // class helpers

    #validateAttachedDocuments = () => {
        // verificar que el objecto que se va a recorrer cumpla esta estructura
        // {
        //     folio: "string",
        //     documentType: "string",
        //     date: moment().format("DD-MM-YYYY"),
        // }


        const isValid = this.attachedDocuments.every(doc => {
            return typeof doc.folio === "string" &&
                typeof doc.documentType === "string" &&
                moment(doc.date, "DD-MM-YYYY", true).isValid();
        });

        if (!isValid) {
            throw {
                code: 400,
                error: "Bad Request",
                message: "Attached documents must have a valid structure: { folio: 'string', documentType: 'string', date: 'DD-MM-YYYY' }"
            };
        }
    }
}

export default Bill;

