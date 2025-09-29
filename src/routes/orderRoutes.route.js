import { Router } from "express";

const router = Router();

// Define your order-related routes here
router.get('/createOrderEmailBody', (req, res) => {
    try {
        const { customData } = req.body;

        const { email, qtyPink, qtyDulce, qtyAmargo, shippingAddress, rut } = customData;

        //validate required fields each one
        if (!email || !qtyPink || !qtyDulce || !qtyAmargo || !shippingAddress || !rut) {
            return res.status(400).json({ message: 'Missing required fields' });
        }

        const emailBody = `Hola, he realizado un nuevo pedido con los siguientes detalles:
        - RUT: ${rut}
        - Dirección de envío: ${shippingAddress}
        - Correo electrónico: ${email}
        - Cantidad de Franui Pink: ${qtyPink}
        - Cantidad de Franui Dulce: ${qtyDulce}
        - Cantidad de Franui Amargo: ${qtyAmargo}`

        res.json({ emailBody });
    } catch (error) {
        console.error('Error generating order email body:', error);
        res.status(500).json({ message: 'Internal server error' });
    }

});

export default router;