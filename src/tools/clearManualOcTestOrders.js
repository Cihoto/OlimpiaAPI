import 'dotenv/config';
import fs from 'fs';
import { MongoClient } from 'mongodb';

const DEFAULT_DB_NAME = 'Olimpia';
const DEFAULT_COLLECTION = 'manual_oc_logs';

function toArray(value) {
    if (!value) return [];
    return String(value)
        .split(',')
        .map((part) => part.trim())
        .filter(Boolean);
}

function normalizeOrderNumber(value) {
    const cleaned = String(value || '')
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '')
        .trim();
    if (!cleaned) return '';
    const withPrefix = cleaned.startsWith('PO') ? cleaned : `PO${cleaned}`;
    return /^PO\d{5,}$/.test(withPrefix) ? withPrefix : '';
}

function parseArgs(argv) {
    const args = {
        ocs: [],
        manualOcIds: [],
        source: '',
        dryRun: true,
        file: ''
    };

    for (let index = 0; index < argv.length; index += 1) {
        const token = argv[index];
        if (token === '--oc') {
            args.ocs.push(...toArray(argv[index + 1]));
            index += 1;
            continue;
        }
        if (token === '--manual-oc-id') {
            args.manualOcIds.push(...toArray(argv[index + 1]));
            index += 1;
            continue;
        }
        if (token === '--source') {
            args.source = String(argv[index + 1] || '').trim().toUpperCase();
            index += 1;
            continue;
        }
        if (token === '--file') {
            args.file = String(argv[index + 1] || '').trim();
            index += 1;
            continue;
        }
        if (token === '--yes') {
            args.dryRun = false;
            continue;
        }
        if (token === '--dry-run') {
            args.dryRun = true;
            continue;
        }
    }

    if (args.file) {
        const raw = fs.readFileSync(args.file, 'utf8');
        const tokens = raw
            .split(/\r?\n/)
            .map((line) => line.trim())
            .filter(Boolean);
        args.ocs.push(...tokens);
    }

    const normalizedOcSet = new Set(
        args.ocs
            .map((value) => normalizeOrderNumber(value))
            .filter(Boolean)
    );
    const normalizedManualOcIdSet = new Set(
        args.manualOcIds
            .map((value) => String(value || '').trim())
            .filter(Boolean)
    );

    return {
        ocs: Array.from(normalizedOcSet),
        manualOcIds: Array.from(normalizedManualOcIdSet),
        source: args.source,
        dryRun: args.dryRun
    };
}

function buildFilter({ ocs, manualOcIds, source }) {
    const orClauses = [];
    if (ocs.length > 0) {
        orClauses.push({ detectedOrderNumber: { $in: ocs } });
    }
    if (manualOcIds.length > 0) {
        orClauses.push({ manualOcId: { $in: manualOcIds } });
    }

    if (orClauses.length === 0) {
        throw new Error('Debes indicar al menos una OC (--oc) o manualOcId (--manual-oc-id).');
    }

    const filter = orClauses.length === 1 ? orClauses[0] : { $or: orClauses };
    if (source) {
        filter.sourceClientCode = source;
    }
    return filter;
}

function printUsage() {
    console.log('Uso: node src/tools/clearManualOcTestOrders.js --oc PO12345,PO67890 [--source PEYA] [--yes]');
    console.log('     node src/tools/clearManualOcTestOrders.js --manual-oc-id <id1,id2> [--yes]');
    console.log('     node src/tools/clearManualOcTestOrders.js --file ./tmp/ocs.txt [--source PEYA] [--yes]');
    console.log('Por defecto corre en dry-run. Usa --yes para eliminar.');
}

async function main() {
    const parsed = parseArgs(process.argv.slice(2));
    const mongoUri = process.env.MONGO_URI;
    const dbName = process.env.MONGO_DB_NAME || DEFAULT_DB_NAME;
    const collectionName = process.env.MONGO_MANUAL_OC_COLLECTION || DEFAULT_COLLECTION;

    if (!mongoUri) {
        throw new Error('MONGO_URI no esta definido en .env');
    }

    const filter = buildFilter(parsed);

    console.log('--- clearManualOcTestOrders ---');
    console.log(`DB: ${dbName}`);
    console.log(`Collection: ${collectionName}`);
    console.log(`Source: ${parsed.source || '(sin filtro)'}`);
    console.log(`OCs: ${parsed.ocs.length ? parsed.ocs.join(', ') : '(ninguna)'}`);
    console.log(`manualOcIds: ${parsed.manualOcIds.length ? parsed.manualOcIds.join(', ') : '(ninguno)'}`);
    console.log(`Modo: ${parsed.dryRun ? 'dry-run' : 'DELETE'}`);

    const client = new MongoClient(mongoUri);
    try {
        await client.connect();
        const collection = client.db(dbName).collection(collectionName);

        const docs = await collection
            .find(filter, { projection: { _id: 0, manualOcId: 1, detectedOrderNumber: 1, sourceClientCode: 1, status: 1, createdAt: 1 } })
            .sort({ createdAt: -1 })
            .toArray();

        console.log(`Registros encontrados: ${docs.length}`);
        for (const doc of docs.slice(0, 50)) {
            console.log(`- manualOcId=${doc.manualOcId || '-'} | OC=${doc.detectedOrderNumber || '-'} | source=${doc.sourceClientCode || '-'} | status=${doc.status || '-'}`);
        }
        if (docs.length > 50) {
            console.log(`... y ${docs.length - 50} más`);
        }

        if (parsed.dryRun) {
            console.log('Dry-run finalizado. No se eliminaron registros.');
            return;
        }

        if (docs.length === 0) {
            console.log('No hay registros para eliminar.');
            return;
        }

        const deleteResult = await collection.deleteMany(filter);
        console.log(`Eliminados: ${deleteResult.deletedCount}`);
    } finally {
        await client.close();
    }
}

if (process.argv.includes('--help') || process.argv.includes('-h')) {
    printUsage();
    process.exit(0);
}

main().catch((error) => {
    console.error(`Error limpiando manual OCs de test: ${error.message}`);
    process.exit(1);
});

