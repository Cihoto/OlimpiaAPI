import { Router } from "express";
const router = Router();

router.post('/sendMailToWebHook', async (req, res) => {
    const {email} = req.body;
    const response = await fetch ('https://services.leadconnectorhq.com/hooks/vdenYRGQMAGUqXLAvk8N/webhook-trigger/ce76f359-1e6c-4fd1-894d-e5f57d67b533', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(email)
    });
    const data = await response.json();
    console.log("data", data);
    res.status(200).json(data);
    return
});



export default router;