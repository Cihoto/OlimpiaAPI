const { google } = require('googleapis');
const path = require('path');
const fs = require('fs');

// Ruta al archivo de credenciales
const CREDENTIALS_PATH = path.join(__dirname, '../../config/google-credentials.json');

// Autenticación con Google
async function authenticateGoogle() {
    const credentials = JSON.parse(fs.readFileSync(CREDENTIALS_PATH, 'utf8'));
    const auth = new google.auth.GoogleAuth({
        credentials,
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    return auth.getClient();
}

// Leer datos de una hoja de cálculo
async function readSheet(spreadsheetId, range) {
    const auth = await authenticateGoogle();
    const sheets = google.sheets({ version: 'v4', auth });

    const response = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range,
    });

    return response.data.values;
}

// Exportar funciones
module.exports = {
    readSheet,
};
