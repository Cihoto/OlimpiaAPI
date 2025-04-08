import fs from 'fs';
import csvParser from 'csv-parser';

const CSV = './src/documents/olimpia_mail.csv'; // Use the file path as a string

async function readCSV(req, res) {
    const results = [];
    const {rutToSearch} = req.query; // Get the RUT from the query parameters

    const normalizedRut = normalizeRut(rutToSearch); // Normalize the RUT
    console.log(`RUT to search: ${normalizedRut}`); // Log the RUT to search
    try {
        fs.createReadStream(CSV)
            .pipe(csvParser())
            .on('data', (data) => {
                if(data.RUT == normalizedRut){
                    results.push(data);

                } // Collect all rows
            })
            .on('end', () => {
                console.log(results);
                res.status(200).json(results); // Return all data
            });
    } catch (error) {
        res.status(500).json({ error: 'Error reading the CSV file' });
    }
}

function normalizeRut(rut) {
    // Remove all non-numeric characters except 'k' or 'K' (used in Chilean RUTs)
    rut = rut.replace(/[^0-9kK]/g, '');

    // Convert to uppercase for consistency
    rut = rut.toUpperCase();

    // Separate the RUT into the body and the verifier digit
    const body = rut.slice(0, -1);
    const verifier = rut.slice(-1);

    // Format the RUT with thousands separator and append the verifier digit
    const formattedRut = body.replace(/\B(?=(\d{3})+(?!\d))/g, '.') + '-' + verifier;

    return formattedRut;
    
}


function searchByAddress(res,req) {
    const {data, address} = req.query; // Get the address from the query parameters
    console.log(`Address to search: ${address}`); // Log the address to search
    const bestMatch = data.reduce((best, current) => {
        const similarity = getSimilarity(current.address, address);
        return similarity > best.similarity ? { similarity, record: current } : best;
    }, { similarity: 0, record: null });

    if (bestMatch.record) {
        res.status(200).json(bestMatch.record);
    } else {
        res.status(404).json({ error: 'No matching address found' });
    }

function getSimilarity(str1, str2) {
    const [longer, shorter] = str1.length > str2.length ? [str1, str2] : [str2, str1];
    const longerLength = longer.length;
    if (longerLength === 0) return 1.0;
    const editDistance = getEditDistance(longer, shorter);
    return (longerLength - editDistance) / longerLength;
}

function getEditDistance(a, b) {
    const matrix = Array.from({ length: b.length + 1 }, (_, i) => Array(a.length + 1).fill(i));
    for (let i = 0; i <= a.length; i++) matrix[0][i] = i;

    for (let i = 1; i <= b.length; i++) {
        for (let j = 1; j <= a.length; j++) {
            const cost = a[j - 1] === b[i - 1] ? 0 : 1;
            matrix[i][j] = Math.min(
                matrix[i - 1][j] + 1,
                matrix[i][j - 1] + 1,
                matrix[i - 1][j - 1] + cost
            );
        }
    }
    return matrix[b.length][a.length];
}
}

export { readCSV };