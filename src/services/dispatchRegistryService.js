import { normalizeBranchName } from '../utils/branchIdentity.js';
import {
    FACTURADO_STATUS,
    normalizeRut,
    sendNotificationEmail
} from './invoiceNotificationEmail.js';
import {
    findDispatchRecordByInvoiceId,
    findDispatchCollisions,
    insertDispatchRecord
} from './mongoDispatchRecordsRegistry.js';

const DEFAULT_SOURCE = 'unknown';

function pickFirst(...values) {
    for (const value of values) {
        if (value !== undefined && value !== null && value !== '') {
            return value;
        }
    }
    return null;
}

function toIsoDate(value) {
    if (!value) return null;
    const s = String(value).trim();
    if (/^\d{4}-\d{2}-\d{2}/.test(s)) {
        return s.slice(0, 10);
    }
    return null;
}

// Normaliza la lista de items a un shape uniforme { code, name, quantity },
// prefiriendo BILLJSON.details (enriquecido por Defontana con productName) sobre
// BillingData.details (solo código + cantidad). Filtra items con cantidad 0,
// ya que el formulario suele incluir todos los SKUs como placeholders.
function normalizeLineItems(billJsonDetails, billingDetails) {
    const source = Array.isArray(billJsonDetails) && billJsonDetails.length > 0
        ? billJsonDetails
        : (Array.isArray(billingDetails) ? billingDetails : []);

    return source
        .map((item) => {
            const quantity = Number(item?.count ?? item?.quantity ?? 0);
            const name = String(item?.productName || item?.name || '').trim();
            return {
                code: String(item?.code || ''),
                name: name || null,
                quantity: Number.isFinite(quantity) ? quantity : 0
            };
        })
        .filter((item) => item.quantity > 0);
}

function sumQuantity(lineItems) {
    return lineItems.reduce((sum, item) => sum + item.quantity, 0);
}

function resolveDeliveryType(isDelivery) {
    return isDelivery === true || isDelivery === 'true' ? 'DESPACHO' : 'RETIRO_BODEGA';
}

// Construye un dispatch_record a partir del envelope { BillingData, ClientData } y
// el invoiceId resuelto desde la respuesta de Defontana. Devuelve null cuando
// faltan campos críticos (invoiceId, RUT, nombre de sucursal, fecha de despacho),
// señalizando al caller que no debe persistir nada.
//
// Función pura: no agrega createdAt ni accede a DB. La capa de persistencia
// completa esos campos al insertar.
function buildDispatchRecord({ billingData, clientData, billJson, invoiceId, source = DEFAULT_SOURCE }) {
    if (!invoiceId) {
        return null;
    }

    const billing = billingData || {};
    const cdata = clientData?.data || {};

    const clientRutRaw = pickFirst(billing.clientFile, cdata.RUT);
    const clientRut = normalizeRut(clientRutRaw);
    const branchNameRaw = pickFirst(cdata.NAME);

    if (!clientRut || !branchNameRaw) {
        return null;
    }

    const dispatchDate = toIsoDate(billing.deliveryDay) || toIsoDate(cdata.deliveryDay);
    if (!dispatchDate) {
        return null;
    }

    const branchName = String(branchNameRaw).trim();
    const clientName = pickFirst(cdata['RAZÓN SOCIAL'], cdata['RAZON SOCIAL']);
    const details = normalizeLineItems(billJson?.details, billing.details);

    return {
        invoiceId: String(invoiceId),
        source: source || DEFAULT_SOURCE,
        reservationId: pickFirst(billing.reservationId),

        clientRut,
        clientRutRaw: clientRutRaw ? String(clientRutRaw) : null,
        clientName: clientName ? String(clientName).trim() : null,

        branchName,
        branchNameNormalized: normalizeBranchName(branchName),
        branchAddress: pickFirst(cdata['Direccion Despacho'], cdata['Dirección Despacho']),
        branchComuna: pickFirst(cdata['Comuna Despacho']),
        branchRegion: pickFirst(cdata.region, billing.region),
        branchRegionLabel: pickFirst(cdata['Region Despacho'], cdata['Región Despacho']),

        dispatchDate,
        deliveryType: resolveDeliveryType(billing.isDelivery),

        documentType: pickFirst(billing.documentType),
        paymentCondition: pickFirst(billing.paymentCondition),
        details,
        totalUnits: sumQuantity(details),

        externalDocumentID: pickFirst(billing.externalDocumentID),
        matchConfidence: clientData?.matchConfidence ?? null,
        matchMinConfidence: clientData?.matchMinConfidence ?? null
    };
}

function formatLineItem(item) {
    const label = item.name ? item.name : `cod. ${item.code}`;
    return `      - ${item.quantity}x ${label}`;
}

// Escapa contenido dinámico antes de interpolar en el HTML del correo,
// para evitar que un nombre de sucursal/producto con caracteres especiales
// rompa el layout o introduzca contenido no deseado.
function escapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

const MONTHS_ES = [
    'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
    'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'
];
const WEEKDAYS_ES = [
    'domingo', 'lunes', 'martes', 'miércoles',
    'jueves', 'viernes', 'sábado'
];

// Convierte "2026-05-15" → "Miércoles 15 de mayo, 2026" sin depender de moment locale.
function formatDispatchDateSpanish(isoDate) {
    if (!isoDate) return '';
    const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(isoDate));
    if (!match) return String(isoDate);
    const [, y, m, d] = match.map(Number);
    const date = new Date(Date.UTC(y, m - 1, d));
    const weekday = WEEKDAYS_ES[date.getUTCDay()];
    const month = MONTHS_ES[m - 1];
    const capitalized = weekday.charAt(0).toUpperCase() + weekday.slice(1);
    return `${capitalized} ${d} de ${month}, ${y}`;
}

// Timestamp local Chile (YYYY-MM-DD HH:mm:ss) para anexar al subject del correo.
// Garantiza que cada alerta tenga subject único — evita que Gmail agrupe en threads
// múltiples alertas distintas para la misma sucursal+fecha de despacho.
function getSantiagoTimestamp() {
    const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'America/Santiago',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
    }).formatToParts(new Date());
    const get = (type) => parts.find((p) => p.type === type)?.value || '00';
    return `${get('year')}-${get('month')}-${get('day')} ${get('hour')}:${get('minute')}:${get('second')}`;
}

// Convierte una fecha (Date o ISO string) a "13 may, 10:00 UTC" para uso compacto.
function formatCreatedAtCompact(value) {
    if (!value) return 'pendiente';
    const d = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(d.getTime())) return String(value);
    const day = d.getUTCDate();
    const month = MONTHS_ES[d.getUTCMonth()].slice(0, 3);
    const hh = String(d.getUTCHours()).padStart(2, '0');
    const mm = String(d.getUTCMinutes()).padStart(2, '0');
    return `${day} ${month}, ${hh}:${mm} UTC`;
}

// Paleta Enix oficial (Brandbook). Una sola fuente de verdad para los colores.
const ENIX = {
    morado900: '#332266',
    moradoSoft: '#A390CB',
    naranja900: '#FD7202',
    naranjaSoft: '#FFF0E0',
    cyan900: '#00C7D4',
    amarillo900: '#FFD700',
    gris900: '#373743',
    gris600: '#5C5C70',
    gris300: '#BCBCC8',
    gris100: '#DDDDE3',
    grisBg: '#F5F5F8',
    white: '#FFFFFF'
};

// Stack tipográfico Enix: Nunito principal (Google Fonts) con fallback a system fonts
// para clientes que no carguen webfonts (Outlook desktop). Roboto queda como capa
// intermedia ya que también es del brandbook.
const ENIX_FONT_STACK = `'Nunito','Roboto',-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif`;

function buildHtmlLineItem(item) {
    const label = item.name
        ? escapeHtml(item.name)
        : `<span style="font-family:Menlo,Monaco,Consolas,monospace;color:${ENIX.gris600};">cod. ${escapeHtml(item.code)}</span>`;
    return `
      <tr>
        <td style="padding:5px 0;font-size:14px;color:${ENIX.gris900};font-family:${ENIX_FONT_STACK};">
          <span style="display:inline-block;min-width:34px;color:${ENIX.morado900};font-weight:700;">${item.quantity}×</span>${label}
        </td>
      </tr>`;
}

function buildHtmlRecordCard(record, isNew) {
    const items = Array.isArray(record.details) ? record.details : [];
    const itemsHtml = items.length === 0
        ? `<tr><td style="padding:4px 0;font-size:13px;color:${ENIX.gris300};font-style:italic;font-family:${ENIX_FONT_STACK};">(sin detalle de productos)</td></tr>`
        : items.map(buildHtmlLineItem).join('');

    const newBadge = isNew
        ? `<span style="display:inline-block;padding:4px 9px;background:${ENIX.naranja900};color:${ENIX.white};border-radius:4px;font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:0.7px;font-family:${ENIX_FONT_STACK};">Recién registrado</span>`
        : '';

    const cardBorder = isNew ? ENIX.naranja900 : ENIX.gris100;
    const cardBg = isNew ? ENIX.naranjaSoft : ENIX.white;
    const innerDivider = isNew ? '#F5D9BC' : ENIX.gris100;

    return `
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom:12px;border:1px solid ${cardBorder};border-radius:8px;background:${cardBg};">
          <tr>
            <td style="padding:18px 20px;">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                <tr>
                  <td style="font-size:14px;font-weight:700;color:${ENIX.morado900};font-family:Menlo,Monaco,Consolas,monospace;word-break:break-all;">${escapeHtml(record.invoiceId)}</td>
                  <td align="right" style="padding-left:8px;white-space:nowrap;">${newBadge}</td>
                </tr>
              </table>
              <div style="margin-top:10px;font-size:13px;color:${ENIX.gris600};font-family:${ENIX_FONT_STACK};">
                <span style="display:inline-block;padding:3px 9px;background:${ENIX.gris100};color:${ENIX.gris900};border-radius:4px;font-weight:700;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">${escapeHtml(record.deliveryType || '-')}</span>
                &nbsp;·&nbsp; source: ${escapeHtml(record.source || 'unknown')}
                &nbsp;·&nbsp; ${escapeHtml(formatCreatedAtCompact(record.createdAt))}
              </div>
              <div style="margin-top:14px;padding-top:14px;border-top:1px solid ${innerDivider};">
                <div style="font-size:11px;text-transform:uppercase;color:${ENIX.gris600};letter-spacing:0.7px;font-weight:700;margin-bottom:8px;font-family:${ENIX_FONT_STACK};">Productos</div>
                <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                  ${itemsHtml}
                </table>
              </div>
            </td>
          </tr>
        </table>`;
}

// Construye el HTML del correo de alerta. Email-safe: layout con tablas,
// estilos inline, sin recursos externos. Renderiza bien en Gmail, Outlook,
// Apple Mail, clientes móviles.
function buildDispatchAlertEmailHtml({ newRecord, collisions }) {
    const allRecords = [...(collisions || []), newRecord];
    const totalCount = allRecords.length;
    const dispatchDateFormatted = formatDispatchDateSpanish(newRecord.dispatchDate);
    const preheader = `${newRecord.branchName} tiene ${totalCount} pedidos confirmados para el ${dispatchDateFormatted}.`;

    const cardsHtml = allRecords
        .map((r) => buildHtmlRecordCard(r, r.invoiceId === newRecord.invoiceId))
        .join('');

    return `<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Despacho duplicado</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&display=swap" rel="stylesheet">
<!--[if mso]>
<style type="text/css">
table, td, div, h1, p { font-family: 'Segoe UI', Arial, sans-serif !important; }
</style>
<![endif]-->
</head>
<body style="margin:0;padding:0;background:${ENIX.grisBg};font-family:${ENIX_FONT_STACK};color:${ENIX.gris900};">
  <div style="display:none;font-size:1px;color:#fefefe;line-height:1px;max-height:0;max-width:0;opacity:0;overflow:hidden;mso-hide:all;">${escapeHtml(preheader)}</div>
  <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:${ENIX.grisBg};">
    <tr>
      <td align="center" style="padding:32px 12px;">

        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="620" style="max-width:620px;width:100%;background:${ENIX.white};border-radius:12px;border:1px solid ${ENIX.gris100};overflow:hidden;">

          <tr>
            <td style="padding:0;background:${ENIX.morado900};background-image:linear-gradient(135deg,${ENIX.morado900} 0%,#43308a 100%);">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                <tr>
                  <td style="padding:24px 32px 20px;">
                    <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                      <tr>
                        <td style="vertical-align:middle;">
                          <img src="https://assets.cdn.filesafe.space/879wY8i5Fg4DZh2cIH8t/media/663b1255b478502a0c2c9ccd.png" width="160" height="auto" alt="Enix" style="display:block;border:0;outline:none;text-decoration:none;max-height:36px;width:auto;">
                        </td>
                        <td align="right" style="vertical-align:middle;">
                          <span style="display:inline-block;padding:5px 12px;background:rgba(253,114,2,0.18);color:${ENIX.naranja900};border-radius:20px;font-family:${ENIX_FONT_STACK};font-size:10px;font-weight:800;letter-spacing:1.5px;text-transform:uppercase;">Alertas</span>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <tr>
            <td style="padding:36px 36px 12px;">
              <div style="display:inline-block;padding:5px 11px;background:${ENIX.naranjaSoft};color:${ENIX.naranja900};border-radius:4px;font-family:${ENIX_FONT_STACK};font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:1px;">Detección</div>
              <h1 style="margin:14px 0 0;font-family:${ENIX_FONT_STACK};font-size:26px;line-height:1.25;color:${ENIX.morado900};font-weight:800;">Despacho duplicado</h1>
              <p style="margin:10px 0 0;color:${ENIX.gris600};font-family:${ENIX_FONT_STACK};font-size:14px;line-height:1.55;">Una sucursal tiene múltiples pedidos confirmados para la misma fecha de despacho.</p>
            </td>
          </tr>

          <tr>
            <td style="padding:24px 36px 8px;">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border:1px solid ${ENIX.gris100};border-radius:8px;background:${ENIX.grisBg};">
                <tr>
                  <td style="padding:18px 20px;">
                    <div style="font-family:${ENIX_FONT_STACK};font-size:10px;text-transform:uppercase;color:${ENIX.gris600};letter-spacing:1px;font-weight:800;">Cliente</div>
                    <div style="margin-top:4px;font-family:${ENIX_FONT_STACK};font-size:16px;color:${ENIX.gris900};font-weight:700;">${escapeHtml(newRecord.clientName || '(sin razón social)')}</div>
                    <div style="margin-top:2px;font-family:${ENIX_FONT_STACK};font-size:13px;color:${ENIX.gris600};">RUT ${escapeHtml(newRecord.clientRutRaw || newRecord.clientRut)}</div>
                  </td>
                </tr>
                <tr><td style="padding:0 20px;"><div style="border-top:1px solid ${ENIX.gris100};"></div></td></tr>
                <tr>
                  <td style="padding:18px 20px;">
                    <div style="font-family:${ENIX_FONT_STACK};font-size:10px;text-transform:uppercase;color:${ENIX.gris600};letter-spacing:1px;font-weight:800;">Sucursal</div>
                    <div style="margin-top:4px;font-family:${ENIX_FONT_STACK};font-size:16px;color:${ENIX.gris900};font-weight:700;">${escapeHtml(newRecord.branchName)}</div>
                    <div style="margin-top:2px;font-family:${ENIX_FONT_STACK};font-size:13px;color:${ENIX.gris600};">${escapeHtml(newRecord.branchComuna || '-')}, ${escapeHtml(newRecord.branchRegionLabel || newRecord.branchRegion || '-')}</div>
                  </td>
                </tr>
                <tr><td style="padding:0 20px;"><div style="border-top:1px solid ${ENIX.gris100};"></div></td></tr>
                <tr>
                  <td style="padding:18px 20px;">
                    <div style="font-family:${ENIX_FONT_STACK};font-size:10px;text-transform:uppercase;color:${ENIX.gris600};letter-spacing:1px;font-weight:800;">Fecha de despacho</div>
                    <div style="margin-top:4px;font-family:${ENIX_FONT_STACK};font-size:16px;color:${ENIX.morado900};font-weight:700;">${escapeHtml(dispatchDateFormatted)}</div>
                    <div style="margin-top:2px;font-family:${ENIX_FONT_STACK};font-size:13px;color:${ENIX.gris600};">${totalCount} pedidos confirmados para esta sucursal</div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <tr>
            <td style="padding:28px 36px 16px;">
              <div style="margin:0 0 14px;font-family:${ENIX_FONT_STACK};font-size:11px;text-transform:uppercase;letter-spacing:1px;color:${ENIX.gris600};font-weight:800;">Pedidos involucrados</div>
              ${cardsHtml}
            </td>
          </tr>

          <tr>
            <td style="padding:22px 36px;background:${ENIX.grisBg};border-top:1px solid ${ENIX.gris100};">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                <tr>
                  <td style="font-family:${ENIX_FONT_STACK};font-size:13px;color:${ENIX.gris600};line-height:1.6;">
                    <strong style="color:${ENIX.gris900};font-weight:700;">Esta alerta es solo informativa.</strong> Los pedidos siguen su curso normal de facturación. Verificar con el cliente si se trata de un duplicado por error o un caso legítimo (re-stock, urgencia, etc.).
                  </td>
                </tr>
              </table>
            </td>
          </tr>

        </table>

        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="620" style="max-width:620px;">
          <tr>
            <td align="center" style="padding:20px 12px 4px;">
              <div style="font-family:${ENIX_FONT_STACK};font-size:11px;font-weight:700;color:${ENIX.morado900};letter-spacing:1.5px;text-transform:uppercase;">Enix · Renacer Digital</div>
              <div style="margin-top:4px;font-family:${ENIX_FONT_STACK};font-size:10px;color:${ENIX.gris600};">Sistema automático de detección de despachos duplicados</div>
            </td>
          </tr>
        </table>

      </td>
    </tr>
  </table>
</body>
</html>`;
}

function formatRecordLine(record, marker = '') {
    const tag = marker ? ` ${marker}` : '';
    const created = record.createdAt
        ? new Date(record.createdAt).toISOString()
        : 'pendiente';
    const items = Array.isArray(record.details) ? record.details : [];
    const productsBlock = items.length === 0
        ? '    productos: (sin detalle)'
        : ['    productos:', ...items.map(formatLineItem)].join('\n');

    return [
        `  - invoiceId: ${record.invoiceId}${tag}`,
        `    source: ${record.source}`,
        `    tipo: ${record.deliveryType}`,
        productsBlock,
        `    registrado: ${created}`
    ].join('\n');
}

// Construye el correo de alerta cuando hay >=2 dispatch_records con la misma
// (clientRut, branchNameNormalized, dispatchDate). Función pura.
function buildDispatchAlertEmail({ newRecord, collisions }) {
    const allRecords = [...(collisions || []), newRecord];
    const totalCount = allRecords.length;

    const subject = `[Enix] Despacho duplicado - ${newRecord.clientName || newRecord.clientRut} / ${newRecord.branchName} - ${newRecord.dispatchDate} - ${getSantiagoTimestamp()}`;

    const linesHeader = [
        'ALERTA DE DESPACHO DUPLICADO',
        '',
        `Cliente: ${newRecord.clientName || '(sin razón social)'}  (RUT: ${newRecord.clientRutRaw || newRecord.clientRut})`,
        `Sucursal: ${newRecord.branchName}`,
        `Comuna: ${newRecord.branchComuna || '-'}, ${newRecord.branchRegionLabel || newRecord.branchRegion || '-'}`,
        `Fecha de despacho: ${newRecord.dispatchDate}`,
        '',
        `Esta sucursal tiene ${totalCount} pedidos confirmados para el mismo día:`,
        ''
    ];

    const linesRecords = allRecords.map((r) =>
        formatRecordLine(r, r.invoiceId === newRecord.invoiceId ? '(<- recién registrado)' : '')
    );

    const linesFooter = [
        '',
        'NOTA: Esta alerta es solo informativa. Los pedidos continúan su curso',
        'normal de facturación. Verifica con el cliente si se trata de un duplicado',
        'por error o un caso legítimo (re-stock, urgencia, etc.).'
    ];

    const text = [...linesHeader, ...linesRecords, ...linesFooter].join('\n');
    const html = buildDispatchAlertEmailHtml({ newRecord, collisions });

    return { subject, text, html };
}

function isAlertEnabled() {
    const value = String(process.env.DISPATCH_ALERT_ENABLED || 'true').toLowerCase().trim();
    return value !== 'false' && value !== '0' && value !== 'off';
}

// Lee DISPATCH_ALERT_RECIPIENTS (comma-separated). Si no está definido o queda
// vacío, devuelve null para que sendNotificationEmail caiga al fallback global
// MAIL_RECIPIENT. Esto preserva backwards-compat con instalaciones existentes.
function getDispatchAlertRecipients() {
    const raw = String(process.env.DISPATCH_ALERT_RECIPIENTS || '').trim();
    if (!raw) {
        return null;
    }
    const list = raw.split(',').map((s) => s.trim()).filter(Boolean);
    return list.length > 0 ? list : null;
}

// Orquesta el flujo completo: build + idempotencia + insert + detección + alerta.
// Sigue el patrón de maybeSendInvoiceNotification (devuelve un objeto de estado
// describiendo qué ocurrió, sin lanzar excepciones para casos esperados).
//
// El caller debe pasar invoiceStatus e invoiceId ya resueltos por sus helpers
// locales (resolveInvoiceStatus / resolveInvoiceId en billsController), para
// evitar duplicar esa lógica acá.
//
// deps inyectables para tests: el default usa los servicios reales (Mongo + Resend).
async function maybeRecordDispatchAndAlert(
    {
        billingData,
        clientData,
        billJson,
        invoiceId,
        invoiceStatus,
        source = DEFAULT_SOURCE
    },
    deps = {}
) {
    const {
        findByInvoiceId = findDispatchRecordByInvoiceId,
        findCollisions = findDispatchCollisions,
        insertRecord = insertDispatchRecord,
        sendEmail = sendNotificationEmail
    } = deps;

    if (invoiceStatus !== FACTURADO_STATUS) {
        return { attempted: false, reason: 'not_facturado', invoiceStatus };
    }

    if (!invoiceId) {
        return { attempted: false, reason: 'missing_invoice_id' };
    }

    const existing = await findByInvoiceId(invoiceId);
    if (existing) {
        return {
            attempted: true,
            inserted: false,
            duplicate: true,
            reason: 'already_registered',
            invoiceId: String(invoiceId)
        };
    }

    const record = buildDispatchRecord({ billingData, clientData, billJson, invoiceId, source });
    if (!record) {
        return { attempted: true, inserted: false, reason: 'incomplete_record', invoiceId: String(invoiceId) };
    }

    const insertResult = await insertRecord(record);
    if (!insertResult.inserted) {
        // Carrera con otra inserción del mismo invoiceId: el otro caller ya lo registró.
        return {
            attempted: true,
            inserted: false,
            duplicate: true,
            reason: 'race_already_inserted',
            invoiceId: record.invoiceId
        };
    }

    const collisions = await findCollisions({
        clientRut: record.clientRut,
        branchNameNormalized: record.branchNameNormalized,
        dispatchDate: record.dispatchDate,
        excludeInvoiceId: record.invoiceId
    });

    const result = {
        attempted: true,
        inserted: true,
        invoiceId: record.invoiceId,
        collisionsFound: collisions.length,
        alertSent: false,
        alertSkipped: false,
        alertError: null
    };

    if (collisions.length === 0) {
        return result;
    }

    if (!isAlertEnabled()) {
        result.alertSkipped = true;
        result.alertSkipReason = 'DISPATCH_ALERT_ENABLED=false';
        return result;
    }

    try {
        const insertedDoc = insertResult.doc || record;
        const { subject, text, html } = buildDispatchAlertEmail({
            newRecord: insertedDoc,
            collisions
        });
        const emailResult = await sendEmail({
            subject,
            text,
            html,
            recipients: getDispatchAlertRecipients()
        });
        result.alertSent = true;
        result.alertMessageId = emailResult?.messageId || null;
    } catch (error) {
        result.alertError = error?.message || String(error);
    }

    return result;
}

export {
    buildDispatchRecord,
    buildDispatchAlertEmail,
    maybeRecordDispatchAndAlert,
    DEFAULT_SOURCE
};
