import { Router } from "express";

const router = Router();

// Define your order-related routes here
router.post('/createOrderEmailBody', async (req, res) => {
    try {
        console.log("req.body", req.body);
        const { customData } = req.body;
        const { qtyPink, qtyDulce, qtyAmargo, shippingAddress, rut } = customData;

        //validate required fields each one
        if (!shippingAddress || !rut) {
            const missingFields = [];
            if (!shippingAddress) missingFields.push('shippingAddress');
            if (!rut) missingFields.push('rut');
            return res.status(400).json({ message: 'Missing required fields', missingFields });
        }

        function formatChileanRut(rawRut) {
            // Remove all non-digit and non-k/k characters
            let cleanRut = String(rawRut).replace(/[^0-9kK]/g, '').toUpperCase();
            if (cleanRut.length < 2) return rawRut; // Not enough characters to format

            const body = cleanRut.slice(0, -1);
            const dv = cleanRut.slice(-1);

            // Add thousands separator to body
            const formattedBody = body.replace(/\B(?=(\d{3})+(?!\d))/g, '.');

            return `${formattedBody}-${dv}`;
        }

        const formattedRut = formatChileanRut(rut);

        const emailBody = `Hola, quiero realizar un nuevo pedido:\n` +
        `- RUT: ${formattedRut}\n` +
        `- Dirección de despacho: ${shippingAddress}\n`;
        
        let quantities = '';
        if(qtyPink && qtyPink > 0) quantities += `- Cantidad cajas Franui Pink: ${qtyPink}\n`;
        if(qtyDulce && qtyDulce > 0) quantities += `- Cantidad cajas Franui Dulce: ${qtyDulce}\n`;
        if(qtyAmargo && qtyAmargo > 0) quantities += `- Cantidad cajas Franui Amargo: ${qtyAmargo}\n`;

        const finalEmailBody = emailBody + quantities;

        const responseExternalWebhook = await fetch('https://services.leadconnectorhq.com/hooks/Gl52wPdpBISW5fdS3x7A/webhook-trigger/3287054f-0086-4575-9c2d-6e4bead77831', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body:   JSON.stringify({ emailBody: finalEmailBody, rut: formattedRut })
        });
        // const responseExternalWebhookData = await responseExternalWebhook.json();
        // console.log("responseExternalWebhookData", responseExternalWebhookData);
        res.json({ finalEmailBody });

    } catch (error) {
        console.error('Error generating order email body:', error);
        res.status(500).json({ message: 'Internal server error' });
    }

});

export default router;