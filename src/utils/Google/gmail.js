import { google } from 'googleapis';

function requireEnv(name) {
    const value = process.env[name];
    if (!value) {
        throw new Error(`Missing env var: ${name}`);
    }
    return value;
}

function parseScopes(raw) {
    return raw
        .split(',')
        .map((scope) => scope.trim())
        .filter(Boolean);
}

function decodeBase64Url(data) {
    if (!data) {
        return Buffer.from('');
    }

    let base64 = data.replace(/-/g, '+').replace(/_/g, '/');
    while (base64.length % 4 !== 0) {
        base64 += '=';
    }

    return Buffer.from(base64, 'base64');
}

function extractEmailAddress(rawHeader) {
    if (!rawHeader) {
        return '';
    }

    const bracketMatch = rawHeader.match(/<([^>]+)>/);
    if (bracketMatch) {
        return bracketMatch[1].trim();
    }

    const emailMatch = rawHeader.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    return emailMatch ? emailMatch[0] : rawHeader.trim();
}

function headersToMap(headers = []) {
    return headers.reduce((acc, header) => {
        if (header?.name) {
            acc[header.name] = header.value || '';
        }
        return acc;
    }, {});
}

function collectParts(payload) {
    const parts = [];
    const queue = [payload];

    while (queue.length > 0) {
        const current = queue.shift();
        if (!current) {
            continue;
        }
        parts.push(current);
        if (Array.isArray(current.parts)) {
            queue.push(...current.parts);
        }
    }

    return parts;
}

function extractEmailText(payload) {
    if (!payload) {
        return '';
    }

    const parts = collectParts(payload);
    const plainPart = parts.find((part) => part.mimeType === 'text/plain' && part.body?.data);
    if (plainPart) {
        return decodeBase64Url(plainPart.body.data).toString('utf8');
    }

    const htmlPart = parts.find((part) => part.mimeType === 'text/html' && part.body?.data);
    if (htmlPart) {
        const html = decodeBase64Url(htmlPart.body.data).toString('utf8');
        return html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
    }

    if (payload.body?.data) {
        return decodeBase64Url(payload.body.data).toString('utf8');
    }

    return '';
}

function findExcelAttachments(payload) {
    if (!payload) {
        return [];
    }

    const parts = collectParts(payload);
    return parts
        .filter((part) => {
            if (!part?.body?.attachmentId) {
                return false;
            }

            const filename = (part.filename || '').toLowerCase();
            const mimeType = (part.mimeType || '').toLowerCase();

            if (filename.endsWith('.xlsx') || filename.endsWith('.xls')) {
                return true;
            }

            return (
                mimeType === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
                mimeType === 'application/vnd.ms-excel'
            );
        })
        .map((part) => ({
            filename: part.filename || 'attachment',
            mimeType: part.mimeType || '',
            attachmentId: part.body.attachmentId
        }));
}

function findPdfAttachments(payload) {
    if (!payload) {
        return [];
    }

    const parts = collectParts(payload);
    return parts
        .filter((part) => {
            if (!part?.body?.attachmentId) {
                return false;
            }

            const filename = (part.filename || '').toLowerCase();
            const mimeType = (part.mimeType || '').toLowerCase();

            if (filename.endsWith('.pdf')) {
                return true;
            }

            return mimeType === 'application/pdf';
        })
        .map((part) => ({
            filename: part.filename || 'attachment',
            mimeType: part.mimeType || '',
            attachmentId: part.body.attachmentId
        }));
}

async function buildGmailClient() {
    const clientEmail = requireEnv('GOOGLE_SERVICE_ACCOUNT_CLIENT_EMAIL');
    const privateKey = requireEnv('GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY').replace(/\\n/g, '\n');
    const subject = requireEnv('GOOGLE_WORKSPACE_IMPERSONATED_USER');
    const rawScopes = process.env.GOOGLE_GMAIL_SCOPES || 'https://www.googleapis.com/auth/gmail.readonly';
    const scopes = parseScopes(rawScopes);

    const authClient = new google.auth.JWT({
        email: clientEmail,
        key: privateKey,
        scopes,
        subject
    });

    const gmail = google.gmail({ version: 'v1', auth: authClient });
    const userId = process.env.GOOGLE_GMAIL_USER_ID || subject;

    return { gmail, userId };
}

export {
    buildGmailClient,
    decodeBase64Url,
    extractEmailAddress,
    extractEmailText,
    findExcelAttachments,
    findPdfAttachments,
    headersToMap
};
