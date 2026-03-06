import { Resend } from 'resend';

const FACTURADO_STATUS = 'facturado';

function normalizeRut(value) {
    return String(value || '')
        .replace(/[.\-\s]/g, '')
        .toUpperCase()
        .trim();
}

function parseCommaSeparatedValues(value) {
    return String(value || '')
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean);
}

function getAllowedClientRuts() {
    const values = parseCommaSeparatedValues(process.env.NOTIFY_INVOICE_CLIENTS);
    return new Set(values.map(normalizeRut).filter(Boolean));
}

function shouldNotifyInvoice({ rut, status }) {
    const normalizedStatus = String(status || '').toLowerCase().trim();
    if (normalizedStatus !== FACTURADO_STATUS) {
        return false;
    }

    const normalizedRut = normalizeRut(rut);
    if (!normalizedRut) {
        return false;
    }

    const allowedRuts = getAllowedClientRuts();
    if (allowedRuts.size === 0) {
        return false;
    }

    return allowedRuts.has(normalizedRut);
}

function buildInvoiceNotificationEmail({
    razonSocial,
    rut,
    folio,
    monto,
    fechaFacturacion,
    invoiceId,
    origen = 'backend facturacion'
}) {
    const clientName = razonSocial || 'Sin razon social';
    const rutValue = rut || 'Sin RUT';
    const folioValue = folio ?? 'Sin folio';
    const amountValue = monto ?? 'Sin monto';
    const dateValue = fechaFacturacion || new Date().toISOString();
    const invoiceValue = invoiceId || 'Sin invoiceId';

    const subject = `[FACTURADO] Cliente ${clientName} - Folio ${folioValue}`;
    const text = [
        `Cliente: ${clientName}`,
        `RUT: ${rutValue}`,
        `Folio: ${folioValue}`,
        `Monto: ${amountValue}`,
        `Fecha facturacion: ${dateValue}`,
        `InvoiceId: ${invoiceValue}`,
        `Origen: ${origen}`
    ].join('\n');

    return { subject, text };
}

let resendClient = null;

function getResendClient() {
    const apiKey = process.env.RESEND_API_KEY;
    if (!apiKey) {
        throw new Error('RESEND_API_KEY no esta definido en .env');
    }

    if (!resendClient) {
        resendClient = new Resend(apiKey);
    }

    return resendClient;
}

function getMailFrom() {
    const from = String(process.env.MAIL_FROM || '').trim();
    if (!from) {
        throw new Error('MAIL_FROM no esta definido en .env');
    }
    return from;
}

function getMailRecipients() {
    const recipients = parseCommaSeparatedValues(process.env.MAIL_RECIPIENT);
    if (recipients.length === 0) {
        throw new Error('MAIL_RECIPIENT no esta definido en .env');
    }
    return recipients;
}

async function sendNotificationEmail({ subject, text }) {
    if (!subject || !String(subject).trim()) {
        throw new Error('subject es requerido para enviar correo');
    }

    if (!text || !String(text).trim()) {
        throw new Error('text es requerido para enviar correo');
    }

    const resend = getResendClient();
    const from = getMailFrom();
    const recipients = getMailRecipients();

    const { data, error } = await resend.emails.send({
        from,
        to: recipients,
        subject,
        text
    });

    if (error) {
        throw new Error(`Error al enviar correo con Resend: ${error.message || JSON.stringify(error)}`);
    }

    return {
        messageId: data?.id || null,
        fromEmail: from,
        recipientEmail: recipients.join(',')
    };
}

export {
    FACTURADO_STATUS,
    normalizeRut,
    shouldNotifyInvoice,
    buildInvoiceNotificationEmail,
    sendNotificationEmail
};
