import 'dotenv/config';
import { MongoClient } from 'mongodb';

const SOURCE = 'rappi_turbo';
const DEFAULT_DB_NAME = 'Olimpia';
const DEFAULT_COLLECTION = 'init';

function parseArgs(argv) {
    return {
        dryRun: argv.includes('--dry-run')
    };
}

async function main() {
    const { dryRun } = parseArgs(process.argv.slice(2));
    const mongoUri = process.env.MONGO_URI;
    const dbName = process.env.MONGO_DB_NAME || DEFAULT_DB_NAME;
    const collectionName = process.env.MONGO_ORDER_COLLECTION || DEFAULT_COLLECTION;

    if (!mongoUri) {
        throw new Error('MONGO_URI no esta definido en .env');
    }

    const client = new MongoClient(mongoUri);
    try {
        await client.connect();
        const collection = client.db(dbName).collection(collectionName);
        const filter = { source: SOURCE };

        const total = await collection.countDocuments(filter);
        console.log(`Registros encontrados para ${SOURCE}: ${total}`);

        if (dryRun) {
            console.log('Modo dry-run activo: no se eliminaron registros.');
            return;
        }

        if (total === 0) {
            console.log('No hay registros para eliminar.');
            return;
        }

        const result = await collection.deleteMany(filter);
        console.log(`Registros eliminados: ${result.deletedCount}`);
    } finally {
        await client.close();
    }
}

main().catch((error) => {
    console.error(`Error limpiando ordenes rappi_turbo: ${error.message}`);
    process.exit(1);
});
