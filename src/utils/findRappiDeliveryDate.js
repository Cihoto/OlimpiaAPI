import moment from 'moment-timezone';
import findDeliveryDayByComuna from './findDeliveryDate.js';

const RAPPI_TUESDAY_THURSDAY = new Set([
    'la florida',
    'penalolen'
]);

const SANTIAGO_REGION_ALIASES = new Set([
    'santiago',
    'metropolitana',
    'region metropolitana',
    'region metropolitana de santiago',
    'rm'
]);

const SANTIAGO_COMUNAS_REFERENCE = new Set([
    'santiago centro',
    'las condes',
    'providencia',
    'nunoa',
    'vitacura',
    'lo barnechea',
    'estacion central',
    'recoleta',
    'colina',
    'huechuraba',
    'independencia',
    'quilicura',
    'lo espejo',
    'maipu',
    'san bernardo',
    'la florida',
    'penalolen',
    'san miguel',
    'el bosque',
    'la reina',
    'la cisterna',
    'cerrillos',
    'macul',
    'conchali',
    'pudahuel'
]);

function normalizeText(value) {
    return String(value || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .trim();
}

function isSantiagoRegion(region, comuna) {
    const normalizedRegion = normalizeText(region);
    if (SANTIAGO_REGION_ALIASES.has(normalizedRegion)) {
        return true;
    }
    const normalizedComuna = normalizeText(comuna);
    return SANTIAGO_COMUNAS_REFERENCE.has(normalizedComuna);
}

function isTuesdayThursdayComuna(comuna) {
    const normalizedComuna = normalizeText(comuna);
    return (
        RAPPI_TUESDAY_THURSDAY.has(normalizedComuna) ||
        normalizedComuna.includes('florida') ||
        normalizedComuna.includes('penalol')
    );
}

function computeNextDeliveryDate(allowedDays, emailDate) {
    const emailMoment = emailDate
        ? moment.tz(emailDate, 'America/Santiago')
        : moment.tz('America/Santiago');

    const emailDateDayIndex = emailMoment.day();
    const emailDateHour = emailMoment.hour();
    const isWeekend = emailDateDayIndex === 6 || emailDateDayIndex === 0;
    const isFriday = emailDateDayIndex === 5;
    const isAfterCutoff = emailDateHour >= 14;
    const isWeekendLikeBlock = (isFriday && isAfterCutoff) || isWeekend;

    const upcomingDeliveries = [];
    for (let offset = 1; offset <= 14; offset += 1) {
        const candidate = emailMoment.clone().add(offset, 'day');
        if (allowedDays.has(candidate.day())) {
            upcomingDeliveries.push(candidate);
        }
    }

    if (upcomingDeliveries.length === 0) {
        return null;
    }

    let selectedDelivery = upcomingDeliveries[0];

    if (isWeekendLikeBlock) {
        const hasMondayDelivery = allowedDays.has(1);
        if (hasMondayDelivery && selectedDelivery.day() === 1 && upcomingDeliveries[1]) {
            selectedDelivery = upcomingDeliveries[1];
        }
    } else if (isAfterCutoff && upcomingDeliveries[1]) {
        const diffToFirst = upcomingDeliveries[0].diff(emailMoment.clone().startOf('day'), 'days');
        if (diffToFirst === 1) {
            selectedDelivery = upcomingDeliveries[1];
        }
    }

    return selectedDelivery.format('YYYY-MM-DD');
}

function findRappiDeliveryDayByComuna(comunaToSearch, emailDate, region) {
    if (!comunaToSearch || typeof comunaToSearch !== 'string') {
        return null;
    }

    if (!isSantiagoRegion(region, comunaToSearch)) {
        // For regions keep the existing calendarization behavior.
        return findDeliveryDayByComuna(comunaToSearch, emailDate);
    }

    const isTuesdayThursday = isTuesdayThursdayComuna(comunaToSearch);
    const allowedDays = isTuesdayThursday
        ? new Set([2, 4]) // Tuesday, Thursday
        : new Set([1, 3, 5]); // Monday, Wednesday, Friday

    return computeNextDeliveryDate(allowedDays, emailDate);
}

export default findRappiDeliveryDayByComuna;
