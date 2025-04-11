import fs from 'fs';
import csvParser from 'csv-parser';

const CSV = './src/documents/CLIENTES_OLIMPIA.csv'; // Use the file path as a string

async function readCSV(req, res) {
    const results = [];
    const {rutToSearch,address} = req.query; // Get the RUT from the query parameters

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

                results.forEach((item) => {
                    // Normalize prices to numbers
                    item['Precio Caja'] = item['Precio Caja'].replaceAll(',', '');
                    item['Precio Caja'] = item['Precio Caja'].replaceAll('$', '');
                    item['Precio Caja'] = Number(item['Precio Caja']);
                })

                if(results.length == 0) {
                    res.status(200).json({
                        data:[],
                        length: results.length,
                        address: address ? true : false,
                    })
                }

                if(results.length == 1) {
                    // If only one result is found, return it directly
                    res.status(200).json({
                        data: results[0],
                        length: results.length,
                        address: true
                    });
                    return;
                }

                if(results.length > 1 && address) {
                    res.status(200).json({
                        data: JSON.stringify(results),
                        length: results.length,
                        address: true
                    });
                }

                if(results.length > 1 && !address) {
                    // If multiple results are found and no address is provided, return all results
                    res.status(200).json({
                        address: false
                    });
                }

                res.status(200).json(results); // Return all data
            });
    } catch (error) {
        res.status(500).json({ error: 'Error reading the CSV file' });
    }
}
async function readCSV_not(req, res) {
    const results = [];
    // const {rutToSearch} = req.query; // Get the RUT from the query parameters
    const rutToSearch = "76.865.177-9"; // Get the RUT from the query parameters
    const address = "kennedy 5753 local 02"; // Get the address from the query parameters
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

                const findByAddress = searchByAddress(results, address); // Search by address
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


function searchByAddress(data, address) {
    let bestMatch = null;
    let highestScore = 0;

    data.forEach((item) => {
        const currentAddress = item['direccion despacho'] || '';
        const score = calculateSimilarity(currentAddress, address);

        if (score > highestScore) {
            highestScore = score;
            bestMatch = item;
        }
    });

    return bestMatch;
}

function calculateSimilarity(str1, str2) {
    // Simple similarity calculation using common substring length
    const commonLength = [...str1].filter((char, index) => str2[index] === char).length;
    return commonLength / Math.max(str1.length, str2.length);
}




export { readCSV };