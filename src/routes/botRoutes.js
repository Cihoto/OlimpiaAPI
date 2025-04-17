import { Router } from "express";
const router = Router();

router.post('/sendMailToWebHook', async (req, res) => {
    const {email} = req.body;
    const response = await fetch ('https://services.leadconnectorhq.com/hooks/vdenYRGQMAGUqXLAvk8N/webhook-trigger/573f2607-af9c-4e2d-99c0-4801a12f1fb8 ', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(email)
    })
    const data = await response.json();
    console.log("data", data);
    res.status(200).json(data);
    return
})

export default router;