import { google } from 'googleapis';
import fs from 'fs';



// Descargar archivo CSV desde Google Drive
async function downloadCSV(fileId, destinationPath) {
    const auth = await authenticate();
    const drive = google.drive({ version: 'v3', auth });

    const response = await drive.files.get(
        { fileId, alt: 'media' },
        { responseType: 'stream' }
    );

    return new Promise((resolve, reject) => {
        const dest = fs.createWriteStream(destinationPath);
        response.data
            .on('end', () => resolve(`Archivo guardado en ${destinationPath}`))
            .on('error', (err) => reject(err))
            .pipe(dest);
    });
}

// Convertir Google Sheets a CSV
async function exportSheetToCSV(spreadsheetId, range, destinationPath) {
    const auth = await authenticate();
    const sheets = google.sheets({ version: 'v4', auth });

    const response = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range,
    });

    const rows = response.data.values;
    if (!rows || rows.length === 0) {
        throw new Error('No se encontraron datos en la hoja.');
    }

    const csvContent = rows.map((row) => row.join(',')).join('\n');
    fs.writeFileSync(destinationPath, csvContent);
    return `Archivo CSV exportado a ${destinationPath}`;
}

export { downloadCSV, exportSheetToCSV };
