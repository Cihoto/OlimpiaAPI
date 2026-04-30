import moment from 'moment-timezone'; // Import moment-timezone for timezone support
// moment.tz.setDefault('America/Santiago'); // Set default timezone to Chile's timezone

const CHILE_TIMEZONE = 'America/Santiago';
const RM_DELIVERY_CUTOFF_HOUR_RAW = Number.parseInt(
    process.env.DELIVERY_RM_DISPATCH_CUTOFF_HOUR || '12',
    10
);
const REGIONAL_DELIVERY_CUTOFF_HOUR_RAW = Number.parseInt(
    process.env.DELIVERY_REGIONS_DISPATCH_CUTOFF_HOUR
    || process.env.DELIVERY_DISPATCH_CUTOFF_HOUR
    || '13',
    10
);

function normalizeCutoffHour(value, fallback = 13) {
    if (!Number.isFinite(value)) {
        return fallback;
    }
    return Math.max(0, Math.min(23, Math.trunc(value)));
}

const RM_DELIVERY_CUTOFF_HOUR = normalizeCutoffHour(RM_DELIVERY_CUTOFF_HOUR_RAW, 12);
const REGIONAL_DELIVERY_CUTOFF_HOUR = normalizeCutoffHour(REGIONAL_DELIVERY_CUTOFF_HOUR_RAW, 13);

const REGION_V_ALIASES = new Set([
    'v',
    'quinta',
    'quinta region',
    'valparaiso',
    'region de valparaiso'
]);

const REGION_VI_ALIASES = new Set([
    'vi',
    'sexta',
    'sexta region',
    "o'higgins",
    'ohiggins',
    "region del libertador general bernardo o'higgins",
    'region del libertador general bernardo ohiggins'
]);

const REGIONAL_BIWEEKLY_SCHEDULES = Object.freeze({
    V: Object.freeze({
        anchorDate: '2026-03-04' // Wednesday
    }),
    VI: Object.freeze({
        anchorDate: '2026-03-12' // Thursday
    })
});

const BLOCKED_DELIVERY_DATES = new Set([
    '2026-04-03' // Viernes Santo (Chile) - moveable feast, add each year
]);

// Recurring annual holidays (MM-DD, year-independent).
// These block deliveries AND shift the effective preparation day to the next
// working day, so orders cannot be prepared on these dates either.
const ANNUAL_HOLIDAYS = new Set([
    '05-01', // Día del Trabajador
    '05-21', // Glorias Navales
    '09-18', // Fiestas Patrias
    '09-19', // Día de las Glorias del Ejército
    '10-12', // Día del Encuentro de Dos Mundos
    '12-25', // Navidad
]);

const COMUNA_REGION_SCHEDULE_OVERRIDES = new Map([
    // Region V (Valparaíso) – explicit commune list as safety net
    // (also detected via 'Región Despacho' = 'Valparaíso' in client CSV)
    ['curacavi', 'V'],
    ['valparaiso', 'V'],
    ['vina del mar', 'V'],
    ['curauma', 'V'],
    ['concon', 'V'],
    ['renaca', 'V'],
    ['renaca bajo', 'V'],
    ['villa alemana', 'V'],
    ['quilpue', 'V'],
    ['algarrobo', 'V'],
    ['casa blanca', 'V'],
    ['recreo', 'V'],
    ['santo domingo', 'V'],
    ['zapallar', 'V'],
    ['papudo', 'V'],
    ['quillota', 'V'],
    ['la ligua', 'V'],
    ['los andes', 'V'],
    ['san antonio', 'V'],
    ['cartagena', 'V'],
    ['el quisco', 'V'],
    ['el tabo', 'V'],
    ['isla negra', 'V'],
    // Region VI (O'Higgins) – explicit commune list as safety net
    ['buin', 'VI'],
    ['linderos', 'VI'],
    ['rancagua', 'VI'],
    ['machali', 'VI'],
    ['san fernando', 'VI'],
    ['requinoa', 'VI'],
    ['pichilemu', 'VI'],
    ['navidad', 'VI'],
    ['santa cruz', 'VI'],
    ['chimbarongo', 'VI'],
    ['graneros', 'VI'],
    ['coinco', 'VI'],
    ['doñihue', 'VI'],
    ['donihue', 'VI'],
]);

function normalizeText(value) {
    return String(value || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .trim();
}

function normalizeRegionCode(regionValue) {
    const normalized = normalizeText(regionValue).replace(/\s+/g, ' ');
    if (!normalized) {
        return '';
    }
    if (REGION_V_ALIASES.has(normalized)) {
        return 'V';
    }
    if (REGION_VI_ALIASES.has(normalized)) {
        return 'VI';
    }
    return '';
}

function getEmailMoment(emailDate) {
    const parsed = emailDate
        ? moment.tz(emailDate, CHILE_TIMEZONE)
        : moment.tz(CHILE_TIMEZONE);
    if (parsed.isValid()) {
        return parsed;
    }
    return moment.tz(CHILE_TIMEZONE);
}

function isBlockedDeliveryDate(dateValue) {
    if (!dateValue) {
        return false;
    }

    const normalizedDate = moment.isMoment(dateValue)
        ? dateValue.format('YYYY-MM-DD')
        : String(dateValue).trim().slice(0, 10);

    if (BLOCKED_DELIVERY_DATES.has(normalizedDate)) {
        return true;
    }
    // Check recurring annual holidays by MM-DD (year-independent)
    return ANNUAL_HOLIDAYS.has(normalizedDate.slice(5));
}

// Returns true for any day where the warehouse does not operate:
// weekends, specific blocked dates, and recurring annual holidays.
function isNonWorkingDay(dateMoment) {
    const dow = dateMoment.day(); // 0 = Sunday, 6 = Saturday
    return dow === 0 || dow === 6 || isBlockedDeliveryDate(dateMoment);
}

// Advances from startMoment until the first working day (inclusive).
function findNextWorkingDay(startMoment) {
    let d = startMoment.clone().startOf('day');
    for (let guard = 0; guard < 14; guard++) {
        if (!isNonWorkingDay(d)) {
            return d;
        }
        d.add(1, 'day');
    }
    return d; // safety fallback
}

// Advances from startMoment until the first weekday (Mon–Fri), skipping only weekends.
// Holidays do NOT shift the preparation day – they only block delivery dates.
function findNextWeekday(startMoment) {
    let d = startMoment.clone().startOf('day');
    for (let guard = 0; guard < 14; guard++) {
        const dow = d.day();
        if (dow !== 0 && dow !== 6) {
            return d;
        }
        d.add(1, 'day');
    }
    return d; // safety fallback
}

function resolveRegionalScheduleCode(comunaToSearch, region) {
    const normalizedComuna = normalizeText(comunaToSearch);
    const comunaOverride = COMUNA_REGION_SCHEDULE_OVERRIDES.get(normalizedComuna);
    if (comunaOverride) {
        return comunaOverride;
    }

    const regionCode = normalizeRegionCode(region);
    if (regionCode === 'V' || regionCode === 'VI') {
        return regionCode;
    }

    return null;
}

function findNextBiweeklyRegionalDeliveryDay(scheduleCode, emailMoment) {
    const schedule = REGIONAL_BIWEEKLY_SCHEDULES[scheduleCode];
    if (!schedule) {
        return null;
    }

    const anchorMoment = moment.tz(schedule.anchorDate, 'YYYY-MM-DD', CHILE_TIMEZONE).startOf('day');
    if (!anchorMoment.isValid()) {
        return null;
    }

    const orderDayStart = emailMoment.clone().startOf('day');
    // Minute-precision cutoff: an order placed at exactly the cutoff hour (e.g. 13:00:00)
    // is NOT considered late – only orders strictly after that minute are.
    const orderMinutes = emailMoment.hour() * 60 + emailMoment.minute();
    const isAfterCutoff = orderMinutes > REGIONAL_DELIVERY_CUTOFF_HOUR * 60;

    // Business rule: never same-day for region routes.
    let candidate = anchorMoment.clone();
    while (!candidate.isAfter(orderDayStart, 'day')) {
        candidate.add(14, 'days');
    }

    // If order is after cut-off and next route is tomorrow, move to next cycle.
    if (isAfterCutoff) {
        const diffToCandidate = candidate.diff(orderDayStart, 'days');
        if (diffToCandidate === 1) {
            candidate.add(14, 'days');
        }
    }

    let holidayGuard = 0;
    while (isBlockedDeliveryDate(candidate) && holidayGuard < 6) {
        candidate.add(14, 'days');
        holidayGuard += 1;
    }

    return candidate.format('YYYY-MM-DD');
}


const uniqueCommunities = [
    "SANTIAGO CENTRO",
    "LAS CONDES",
    "PROVIDENCIA",
    "ÑUÑOA",
    "VITACURA",
    "LO BARNECHEA",
    "ESTACIÓN CENTRAL",
    "RECOLETA",
    "COLINA",
    "HUECHURABA",
    "INDEPENDENCIA",
    "QUILICURA",
    "LO ESPEJO",
    "MAIPÚ",
    "SAN BERNARDO",
    "LA FLORIDA",
    "PEÑALOLÉN",
    "SAN MIGUEL",
    "EL BOSQUE",
    "LA REINA",
    "LA CISTERNA",
    "CERRILLOS",
    "MACUL",
    "CONCHALÍ",
    "PUDAHUEL"
];

const deliveryDays = [
    {
        index: 1,
        dayName: "LUNES",
        communities: [
            "SANTIAGO CENTRO",
            "LAS CONDES",
            "PROVIDENCIA",
            "ÑUÑOA",
            "VITACURA",
            "LO BARNECHEA",
            "ESTACIÓN CENTRAL",
            "RECOLETA",
            "COLINA",
            "HUECHURABA",
            "INDEPENDENCIA",
            "QUILICURA",
            "PUDAHUEL"
        ]
    },
    {
        index: 2,
        dayName: "MARTES",
        communities: [
            "LO ESPEJO",
            "MAIPÚ",
            "SAN BERNARDO",
            "LA FLORIDA",
            "PEÑALOLÉN",
            "SAN MIGUEL",
            "EL BOSQUE",
            "LA REINA",
            "PROVIDENCIA",
            "LAS CONDES",
            "VITACURA",
            "LA CISTERNA",
            "CERRILLOS",
            "MACUL",
            "ÑUÑOA"
        ] 
    },
    {
        index: 3,
        dayName: "MIÉRCOLES",
        communities: [
            "SANTIAGO CENTRO",
            "LAS CONDES",
            "PROVIDENCIA",
            "ÑUÑOA",
            "VITACURA",
            "LO BARNECHEA",
            "ESTACIÓN CENTRAL",
            "RECOLETA",
            "COLINA",
            "HUECHURABA",
            "PUDAHUEL"
        ]
    },
    {
        index: 4,
        dayName: "JUEVES",
        communities: [
            "LO ESPEJO",
            "MAIPÚ",
            "SAN BERNARDO",
            "LA FLORIDA",
            "PEÑALOLÉN",
            "SAN MIGUEL",
            "EL BOSQUE",
            "LA REINA",
            "PROVIDENCIA",
            "LAS CONDES",
            "VITACURA",
            "LA CISTERNA",
            "CERRILLOS",
            "MACUL",
            "ÑUÑOA",
        ]
    },
    {
        index: 5,
        dayName: "VIERNES",
        communities: [
            "SANTIAGO CENTRO",
            "LAS CONDES",
            "PROVIDENCIA",
            "ÑUÑOA",
            "VITACURA",
            "LO BARNECHEA",
            "ESTACIÓN CENTRAL",
            "RECOLETA",
            "COLINA",
            "HUECHURABA",
            "INDEPENDENCIA",
            "QUILICURA",
            "CONCHALÍ",
            "PUDAHUEL"
        ]
    }
];

function normalizeCommunityName(value) {
    return normalizeText(value);
}

function getDeliveryDayIndexesByComuna(comunaToSearch) {
    if (!comunaToSearch || typeof comunaToSearch !== 'string') {
        return [];
    }

    const normalizedComuna = normalizeCommunityName(comunaToSearch);
    return deliveryDays
        .filter(day => day.communities.some(community => normalizeCommunityName(community) === normalizedComuna))
        .map(day => ({ index: day.index, dayName: day.dayName }))
        .sort((a, b) => a.index - b.index);
}

function getAllDeliveryCommunities() {
    return [...uniqueCommunities];
}

function resolveDispatchCutoffHourByComuna(comunaToSearch, region = '') {
    const regionalScheduleCode = resolveRegionalScheduleCode(comunaToSearch, region);
    if (regionalScheduleCode) {
        return REGIONAL_DELIVERY_CUTOFF_HOUR;
    }
    return RM_DELIVERY_CUTOFF_HOUR;
}

function findDeliveryDayByComuna(comunaToSearch, emailDate, region = '') {

    try {
        // const todayWeekDayIndex = moment().day(); // Obtiene el índice del día de la semana (0 para domingo, 1 para lunes, etc.)
        // Obtiene el índice del día de la semana de la fecha del correo electrónico
        // Obtiene la hora de la fecha del correo electrónico
        // const formattedDate = moment(emailDate).format("YYYY/MM/DD HH:mm:ss");

        if (!comunaToSearch || typeof comunaToSearch !== 'string') {
            return null; // Invalid input
        }
        console.log("comunaToSearch 2", comunaToSearch);

        const emailMoment = getEmailMoment(emailDate);
        const regionalScheduleCode = resolveRegionalScheduleCode(comunaToSearch, region);
        if (regionalScheduleCode) {
            const regionalDeliveryDay = findNextBiweeklyRegionalDeliveryDay(regionalScheduleCode, emailMoment);
            if (regionalDeliveryDay) {
                return regionalDeliveryDay;
            }
        }


        // Check if the comunaToSearch is in the list of unique communities
        const normalizedComuna = normalizeCommunityName(comunaToSearch);
        const isValidCommunity = uniqueCommunities.some(
            community => normalizeCommunityName(community) === normalizedComuna
        );

        if (!isValidCommunity) {
            return null; // Invalid community
        }

        // Find all indexes of the delivery days that match the comunaToSearch
        const deliveryDayIndexes = getDeliveryDayIndexesByComuna(comunaToSearch);
        console.log("deliveryDayIndexes", deliveryDayIndexes);

        console.log(`Debe ser traido a la zona horaria de Chile ${emailMoment}`);

        // Minute-precision cutoff check for RM routes
        const orderMinutes = emailMoment.hour() * 60 + emailMoment.minute();
        const isAfterCutoff = orderMinutes > RM_DELIVERY_CUTOFF_HOUR * 60;

        // Determine effective preparation day:
        // - After cutoff: cannot start preparing until tomorrow.
        // - Advance past weekends AND holidays: the warehouse cannot prepare
        //   on non-working days, so both shift the preparation day.
        const prepStart = isAfterCutoff
            ? emailMoment.clone().startOf('day').add(1, 'day')
            : emailMoment.clone().startOf('day');
        const effectivePreparationDay = findNextWorkingDay(prepStart);

        const deliveryDayIndexSet = new Set(deliveryDayIndexes.map(day => day.index));
        const upcomingDeliveries = [];

        for (let offset = 1; offset <= 14; offset++) {
            const candidate = emailMoment.clone().add(offset, 'day');
            if (deliveryDayIndexSet.has(candidate.day())
                && !isBlockedDeliveryDate(candidate)
                && candidate.isAfter(effectivePreparationDay, 'day')) {
                upcomingDeliveries.push(candidate);
            }
        }

        if (upcomingDeliveries.length === 0) {
            return null;
        }

        const selectedDelivery = upcomingDeliveries[0];

        const formattedDate = selectedDelivery.format("YYYY-MM-DD");
        console.log("date", formattedDate);
        return formattedDate;
        // return {deliveryIndex, moment(deliveryIndex).format("YYYY/MM/DD HH:mm:ss")};
    } catch (e) {
        console.log("error", e)
        return null;
    }
}

function DeliveryDaySelector(deliveryDayIndexes, i, emailDateFormatted, emailDateHour) {
    const daysToNextDelivery = diffToNextDeliveryDay(deliveryDayIndexes, i, emailDateFormatted);

    // deliveryIndex = daysToNextDelivery;
    // break

    if (daysToNextDelivery > 2) {
        return moveForward(deliveryDayIndexes.length, i, 0)
        // break;
    }

    if (emailDateHour >= 12) {
        return moveForward(deliveryDayIndexes.length, i, 1)
    } else {
        // console.log({ emailDateHour })
        return moveForward(deliveryDayIndexes.length, i, 0)
    }
}


function moveForward(arrayLength, currentIndex, steps) {

    if (steps == 0) {
        return currentIndex;
    }

    const newIndex = (currentIndex + steps) % arrayLength;
    return newIndex;
}

function diffToNextDeliveryDay(deliveryDayIndexes, nextIndex, orderDate) {

    console.log("*")
    console.log("deliveryDayIndexes", deliveryDayIndexes);
    console.log("nextIndex", nextIndex);
    console.log("orderDate", orderDate);
    console.log("*")

    const todayDayIndex = moment(orderDate).day();
    const todayDeliveryIndexes = deliveryDayIndexes
        .filter(day => day.index === todayDayIndex)
        .map(day => day.index);
    console.log("todayDeliveryIndexes", todayDeliveryIndexes);

    // Buscar el próximo día de entrega en deliveryDayIndexes después de todayDayIndex
    let minDiff = null;

    for (let d of deliveryDayIndexes) {
        let diff = (d.index - todayDayIndex + 7) % 7;
        if (diff === 0) diff = 7; // Si es hoy, cuenta para la próxima semana
        if (minDiff === null || diff < minDiff) {
            minDiff = diff;
        }
    }

    console.log("minDiff", minDiff);
    console.log("minDiff", minDiff);
    console.log("minDiff", minDiff);

    return minDiff;

    const nextDeliveryDay = deliveryDayIndexes[nextIndex % deliveryDayIndexes.length];
    console.log("nextDeliveryDay", nextDeliveryDay);
    // Calculate the next delivery date in the future
    let nextDeliveryDate = moment(orderDate).startOf('day');

    while (nextDeliveryDate.day() !== nextDeliveryDay.index) {
        nextDeliveryDate.add(1, 'day');
    }

    const difference = nextDeliveryDate.diff(moment(orderDate).startOf('day'), 'days');
    return difference;
}

// module.exports = findDeliveryDayByComuna; // Replace export default
// with module.exports for CommonJS compatibility
// export default findDeliveryDayByComuna; // Uncomment this line if using ES6 modules

export default findDeliveryDayByComuna; // Export the function for use in other files
export { getAllDeliveryCommunities, getDeliveryDayIndexesByComuna, resolveDispatchCutoffHourByComuna };
