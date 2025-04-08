import fs from 'fs';
import csvParser from 'csv-parser';

const CSV = './src/documents/olimpia_mail.csv'; // Use the file path as a string

async function readCSV(req, res) {
    const results = [];
    const {rutToSearch} = req.query; // Get the RUT from the query parameters
    console.log(`RUT to search: ${rutToSearch}`); // Log the RUT to search
    try {
        fs.createReadStream(CSV)
            .pipe(csvParser())
            .on('data', (data) => {
                if(data.RUT == rutToSearch){
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

export { readCSV };