import { Router } from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const jsonFilePath = path.join(__dirname, "../documents/jsonBOT.json");

const router = Router();

router.post('/sendMailToWebHook', async (req, res) => {
    const {email} = req.body;
    console.log("email",email)
    const response = await fetch ('https://services.leadconnectorhq.com/hooks/vdenYRGQMAGUqXLAvk8N/webhook-trigger/ce76f359-1e6c-4fd1-894d-e5f57d67b533', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(req.body)
    });

    writeToJsonFile({
        email: req.body.email,
        url: ""
    });

    const data = await response.json();
    console.log("data", data);
    res.status(200).json({data,email});
    return
});


router.post('/updateurl', async (req, res) => {
    const {email, url} = req.body;

    const updated = updateClientByEmail(email,url)
    res.status(200).json(updated);
    return
});

router.post('/getData', async (req, res) => {
    const {email} = req.body;
    const data = getDataWithMail(email)
    res.json(data);
    return
});

router.post('/deleteData', async (req, res) => {
    const {email} = req.body;
    const data = removeDataFromJson(email)
    res.json(data);
    return
})

const writeToJsonFile = (data) => {
    try {
        const jsonData = fs.existsSync(jsonFilePath) ? fs.readFileSync(jsonFilePath, "utf8") : "[]";
        const parsedData = JSON.parse(jsonData);
        parsedData.push(data);
        fs.writeFileSync(jsonFilePath, JSON.stringify(parsedData, null, 2), "utf8");
        console.log("Archivo JSON actualizado correctamente.");
    } catch (error) {
        console.error("Error al escribir en el archivo JSON:", error);
    }
};

const updateClientByEmail = (email,url) => {
    try {
        const jsonData = fs.readFileSync(jsonFilePath, "utf8");
        const parsedData = JSON.parse(jsonData);
        console.log("parsedData", parsedData)
        const result = parsedData.find(item => item.email == email);
        // return result
        if (result) {
            console.log("Email encontrado:", result);

            result.url = url;
            fs.writeFileSync(jsonFilePath, JSON.stringify(parsedData, null, 2), "utf8");
            return result;
        } else {
            console.log("Email no encontrado.");
            return null;
        }
    } catch (error) {
        console.error("Error al leer el archivo JSON:", error);
        return null;
    }
};

function getDataWithMail(email) {
    try {
        const jsonData = fs.readFileSync(jsonFilePath, "utf8");
        const parsedData = JSON.parse(jsonData);
        // return parsedData
        const result = parsedData.find(item => item.email == email);
        if (result) {
            // console.log("Email encontrado:", result);
            return result;
        } else {
            // console.log("Email no encontrado.");
            return null;
        }
    } catch (error) {
        console.error("Error al leer el archivo JSON:", error);
        return null;
    }
}

function removeDataFromJson(email) {
    try {
        const jsonData = fs.readFileSync(jsonFilePath, "utf8");
        const parsedData = JSON.parse(jsonData);
        const filteredData = parsedData.filter(item => item.email !== email);
        fs.writeFileSync(jsonFilePath, JSON.stringify(filteredData, null, 2), "utf8");
        return true
        console.log("Email eliminado correctamente.");
    } catch (error) {
        return false
        console.error("Error al leer el archivo JSON:", error);
    }
}

export default router;