import fs from 'fs';
import path from 'path';
import moment from 'moment-timezone';
import findDeliveryDayByComuna, {
    getAllDeliveryCommunities,
    getDeliveryDayIndexesByComuna
} from '../utils/findDeliveryDate.js';

const TZ = 'America/Santiago';
const DEFAULT_HOURS = ['09:00', '13:59', '14:00', '14:03', '16:30'];
const WEEKDAY_NAMES = ['DOMINGO', 'LUNES', 'MARTES', 'MIERCOLES', 'JUEVES', 'VIERNES', 'SABADO'];

function parseArgs(argv) {
    const options = {
        month: null,
        hours: DEFAULT_HOURS,
        comuna: null,
        writeCsv: null,
        maxComunas: null,
        showFunctionLogs: false,
        help: false
    };

    for (let i = 0; i < argv.length; i++) {
        const arg = argv[i];
        if (arg === '--help') {
            options.help = true;
            continue;
        }

        if (arg === '--show-function-logs') {
            options.showFunctionLogs = true;
            continue;
        }

        if (arg.startsWith('--month=')) {
            options.month = arg.split('=')[1];
            continue;
        }
        if (arg === '--month') {
            options.month = argv[i + 1] || null;
            i++;
            continue;
        }

        if (arg.startsWith('--hours=')) {
            options.hours = arg.split('=')[1].split(',').map(item => item.trim()).filter(Boolean);
            continue;
        }
        if (arg === '--hours') {
            options.hours = (argv[i + 1] || '').split(',').map(item => item.trim()).filter(Boolean);
            i++;
            continue;
        }

        if (arg.startsWith('--comuna=')) {
            options.comuna = arg.split('=')[1].trim();
            continue;
        }
        if (arg === '--comuna') {
            options.comuna = (argv[i + 1] || '').trim();
            i++;
            continue;
        }

        if (arg.startsWith('--write-csv=')) {
            options.writeCsv = arg.split('=')[1].trim();
            continue;
        }
        if (arg === '--write-csv') {
            options.writeCsv = (argv[i + 1] || '').trim();
            i++;
            continue;
        }

        if (arg.startsWith('--max-comunas=')) {
            options.maxComunas = Number(arg.split('=')[1]);
            continue;
        }
        if (arg === '--max-comunas') {
            options.maxComunas = Number(argv[i + 1] || '');
            i++;
            continue;
        }
    }

    return options;
}

function printHelp() {
    console.log('Uso: node src/tools/deliveryVisibilityReport.js [opciones]');
    console.log('');
    console.log('Opciones:');
    console.log('  --month=YYYY-MM           Mes a evaluar (default: mes actual en America/Santiago)');
    console.log('  --hours=HH:mm,HH:mm       Horas de prueba por dia (default: 09:00,13:59,14:00,14:03,16:30)');
    console.log('  --comuna="NOMBRE"         Probar una comuna especifica (default: todas)');
    console.log('  --max-comunas=N           Limitar cantidad de comunas (util para debugging)');
    console.log('  --write-csv=RUTA          Exportar tambien a CSV');
    console.log('  --show-function-logs      Mostrar logs internos de findDeliveryDayByComuna');
    console.log('  --help                    Mostrar ayuda');
}

function isValidMonth(value) {
    return /^\d{4}-(0[1-9]|1[0-2])$/.test(value);
}

function isValidHour(value) {
    return /^([01]\d|2[0-3]):[0-5]\d$/.test(value);
}

function repairMojibake(value) {
    if (typeof value !== 'string') {
        return value;
    }

    try {
        const repaired = Buffer.from(value, 'latin1').toString('utf8');
        return repaired.includes('�') ? value : repaired;
    } catch (error) {
        return value;
    }
}

function escapeCsv(value) {
    const stringValue = String(value ?? '');
    if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
        return `"${stringValue.replaceAll('"', '""')}"`;
    }
    return stringValue;
}

function main() {
    const options = parseArgs(process.argv.slice(2));
    if (options.help) {
        printHelp();
        process.exit(0);
    }

    const invalidHours = options.hours.filter(hour => !isValidHour(hour));
    if (invalidHours.length > 0) {
        console.error(`Horas invalidas: ${invalidHours.join(', ')}`);
        process.exit(1);
    }

    if (options.month && !isValidMonth(options.month)) {
        console.error('Formato de mes invalido. Usa YYYY-MM, por ejemplo 2026-03');
        process.exit(1);
    }

    const runDeliveryLookup = (comuna, orderIso) => {
        if (options.showFunctionLogs) {
            return findDeliveryDayByComuna(comuna, orderIso);
        }

        const originalConsoleLog = console.log;
        console.log = () => {};
        try {
            return findDeliveryDayByComuna(comuna, orderIso);
        } finally {
            console.log = originalConsoleLog;
        }
    };

    const monthStart = options.month
        ? moment.tz(`${options.month}-01 00:00`, 'YYYY-MM-DD HH:mm', TZ)
        : moment.tz(TZ).startOf('month');
    const daysInMonth = monthStart.daysInMonth();

    let communities = options.comuna ? [options.comuna] : getAllDeliveryCommunities();
    if (Number.isInteger(options.maxComunas) && options.maxComunas > 0) {
        communities = communities.slice(0, options.maxComunas);
    }

    const monthLabel = monthStart.format('YYYY-MM');
    const csvRows = [[
        'comuna',
        'dias_despacho',
        'pedido_local',
        'pedido_weekday',
        'pedido_mode',
        'despacho',
        'despacho_weekday',
        'delta_dias'
    ]];

    console.log('============================================================');
    console.log(`DELIVERY VISIBILITY REPORT`);
    console.log(`Mes: ${monthLabel}`);
    console.log(`Zona horaria: ${TZ}`);
    console.log(`Horas por dia: ${options.hours.join(', ')}`);
    console.log(`Comunas a evaluar: ${communities.length}`);
    console.log('============================================================');

    let totalCases = 0;
    let nullCases = 0;

    for (const comunaRaw of communities) {
        const deliveryDays = getDeliveryDayIndexesByComuna(comunaRaw);
        const hasMondayDelivery = deliveryDays.some(day => day.index === 1);
        const deliveryDayLabel = deliveryDays
            .map(day => `${WEEKDAY_NAMES[day.index]}(${day.index})`)
            .join(', ');
        const comunaDisplay = repairMojibake(comunaRaw);

        console.log('');
        console.log('------------------------------------------------------------');
        console.log(`Comuna: ${comunaDisplay}`);
        console.log(`Dias de despacho: ${deliveryDayLabel || 'SIN DESPACHO'}`);
        console.log(`Tiene lunes: ${hasMondayDelivery ? 'SI' : 'NO'}`);
        console.log('------------------------------------------------------------');

        for (let day = 1; day <= daysInMonth; day++) {
            const dateMoment = monthStart.clone().date(day);
            const dateLabel = dateMoment.format('YYYY-MM-DD');
            const dayLabel = WEEKDAY_NAMES[dateMoment.day()];
            console.log(`${dateLabel} (${dayLabel})`);

            for (const hour of options.hours) {
                const orderMoment = moment.tz(`${dateLabel} ${hour}`, 'YYYY-MM-DD HH:mm', TZ);
                const isFridayAfterCutoff = orderMoment.day() === 5 && orderMoment.hour() >= 14;
                const isWeekend = orderMoment.day() === 6 || orderMoment.day() === 0;
                const mode = isFridayAfterCutoff || isWeekend ? 'VIERNES_FINDE' : 'SEMANA';

                const result = runDeliveryLookup(comunaRaw, orderMoment.toISOString());
                totalCases++;

                if (!result) {
                    nullCases++;
                    console.log(`  ${hour} -> null | modo=${mode}`);
                    csvRows.push([
                        comunaDisplay,
                        deliveryDayLabel,
                        `${dateLabel} ${hour}`,
                        dayLabel,
                        mode,
                        '',
                        '',
                        ''
                    ]);
                    continue;
                }

                const deliveryMoment = moment.tz(result, 'YYYY-MM-DD', TZ);
                const deliveryDayLabelByDate = WEEKDAY_NAMES[deliveryMoment.day()];
                const diffDays = deliveryMoment.diff(orderMoment.clone().startOf('day'), 'days');

                console.log(
                    `  ${hour} -> ${result} (${deliveryDayLabelByDate}) | +${diffDays}d | modo=${mode}`
                );

                csvRows.push([
                    comunaDisplay,
                    deliveryDayLabel,
                    `${dateLabel} ${hour}`,
                    dayLabel,
                    mode,
                    result,
                    deliveryDayLabelByDate,
                    String(diffDays)
                ]);
            }
        }
    }

    console.log('');
    console.log('============================================================');
    console.log(`Total casos: ${totalCases}`);
    console.log(`Resultados null: ${nullCases}`);
    console.log('============================================================');

    if (options.writeCsv) {
        const outputPath = path.isAbsolute(options.writeCsv)
            ? options.writeCsv
            : path.resolve(process.cwd(), options.writeCsv);
        const csvContent = csvRows.map(row => row.map(escapeCsv).join(',')).join('\n');
        fs.writeFileSync(outputPath, csvContent, 'utf8');
        console.log(`CSV generado en: ${outputPath}`);
    }
}

main();
