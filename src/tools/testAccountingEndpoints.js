// GET-only. Validar acceso a endpoints de Accounting de Defontana.
import 'dotenv/config';
import { fetchApiKey, getApiKey } from '../middleware/auth.js';

const ACC_BASE = (process.env.ACCOUNTING_API_URL_PROD || 'https://api.defontana.com/api/Accounting/').replace(/\/+$/, '/');

async function GET(url, apiKey) {
    const r = await fetch(url, { method: 'GET', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` } });
    const text = await r.text();
    let body;
    try { body = JSON.parse(text); } catch { body = text; }
    return { status: r.status, body };
}

async function main() {
    await fetchApiKey();
    const apiKey = getApiKey();
    const rut = process.argv[2] || '96.930.440-6';

    console.log(`Probando endpoints Accounting con RUT=${rut}\n`);

    console.log('--- GetClientCredit ---');
    const cc = await GET(`${ACC_BASE}GetClientCredit?fileID=${encodeURIComponent(rut)}`, apiKey);
    console.log('HTTP', cc.status);
    console.log(JSON.stringify(cc.body, null, 2));

    console.log('\n--- GetDocumentsToPay ---');
    const dtp = await GET(`${ACC_BASE}GetDocumentsToPay?fileID=${encodeURIComponent(rut)}&ItemsPerPage=10&Page=0`, apiKey);
    console.log('HTTP', dtp.status);
    console.log('totalItems:', dtp.body?.totalItems);
    console.log('docs (primeros 5):');
    (dtp.body?.documents || []).slice(0,5).forEach(d => console.log(' ', JSON.stringify(d)));
}

main().catch(e => { console.error(e); process.exit(1); });
