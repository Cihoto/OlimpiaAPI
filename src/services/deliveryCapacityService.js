import { createHash, randomUUID } from 'crypto';
import moment from 'moment-timezone';
import { MongoClient } from 'mongodb';
import { getDeliveryDayIndexesByComuna } from '../utils/findDeliveryDate.js';

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB_NAME || 'Olimpia';
const DAYS_COLLECTION_NAME = process.env.MONGO_DELIVERY_CAPACITY_DAYS_COLLECTION || 'delivery_capacity_days';
const CONFIG_COLLECTION_NAME = process.env.MONGO_DELIVERY_CAPACITY_CONFIG_COLLECTION || 'delivery_capacity_config';
const DEFAULT_MAX_POINTS = Number(process.env.DELIVERY_RM_MAX_POINTS_DEFAULT || 30);
const HOLD_TTL_MINUTES = Number(process.env.DELIVERY_RESERVATION_HOLD_TTL_MINUTES || 12);
const CHILE_TIMEZONE = 'America/Santiago';
const RM_REGION_CODE = 'RM';
const ORDER_POINTS = 1;

const HOLD_STATUS = 'HOLD';
const BILLING_IN_PROGRESS_STATUS = 'BILLING_IN_PROGRESS';
const COMMITTED_STATUS = 'COMMITTED';
const FAILED_STATUS = 'FAILED';
const EXPIRED_STATUS = 'EXPIRED';

const ACTIVE_STATUSES = new Set([
    HOLD_STATUS,
    BILLING_IN_PROGRESS_STATUS,
    COMMITTED_STATUS
]);

let clientPromise = null;
let daysIndexesReady = false;
let configIndexesReady = false;

function toPositiveInteger(value, fallback) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return fallback;
    }
    return Math.floor(parsed);
}

function getDefaultMaxPoints() {
    return toPositiveInteger(DEFAULT_MAX_POINTS, 30);
}

function getHoldTtlMinutes() {
    return toPositiveInteger(HOLD_TTL_MINUTES, 12);
}

function getNowMoment() {
    return moment.tz(CHILE_TIMEZONE);
}

function normalizeRegion(value) {
    return String(value || '').trim().toUpperCase();
}

function normalizeComuna(value) {
    return String(value || '').trim();
}

function getClientField(clientRecord, candidates) {
    const source = clientRecord && typeof clientRecord === 'object' ? clientRecord : {};
    for (const key of candidates) {
        const value = source?.[key];
        if (value !== undefined && value !== null && value !== '') {
            return value;
        }
    }
    return '';
}

function normalizeDate(value) {
    if (!value) {
        return null;
    }

    const valueAsString = String(value).trim();
    if (!valueAsString) {
        return null;
    }

    if (/^\d{4}-\d{2}-\d{2}$/.test(valueAsString)) {
        const parsedDate = moment.tz(valueAsString, 'YYYY-MM-DD', CHILE_TIMEZONE);
        if (parsedDate.isValid()) {
            return parsedDate.format('YYYY-MM-DD');
        }
    }

    const parsedIsoDate = moment(valueAsString, moment.ISO_8601, true);
    if (!parsedIsoDate.isValid()) {
        return null;
    }

    return parsedIsoDate.tz(CHILE_TIMEZONE).format('YYYY-MM-DD');
}

function toPlainObject(value) {
    if (value === undefined || value === null) {
        return {};
    }

    try {
        return JSON.parse(JSON.stringify(value));
    } catch (error) {
        return {};
    }
}

function getOrderQuantities(emailData) {
    const quantities = {};
    const source = emailData && typeof emailData === 'object' ? emailData : {};

    Object.entries(source).forEach(([key, value]) => {
        if (!String(key).startsWith('Pedido_Cantidad_')) {
            return;
        }
        const numericValue = Number(value);
        if (!Number.isFinite(numericValue)) {
            return;
        }
        quantities[key] = numericValue;
    });

    return quantities;
}

function getTotalBoxes(emailData) {
    const quantities = getOrderQuantities(emailData);
    return Object.values(quantities).reduce((acc, value) => acc + Number(value || 0), 0);
}

function buildReservationFingerprint({
    emailData,
    clientRecord,
    requestedDeliveryDay,
    sender,
    emailDate
}) {
    const quantities = getOrderQuantities(emailData);
    const payload = {
        rut: String(emailData?.Rut || clientRecord?.RUT || '').trim().toLowerCase(),
        razonSocial: String(
            emailData?.Razon_social ||
            getClientField(clientRecord, ['RAZÓN SOCIAL', 'RAZÃ“N SOCIAL'])
        ).trim().toLowerCase(),
        comuna: String(
            emailData?.Comuna ||
            getClientField(clientRecord, ['Comuna Despacho'])
        ).trim().toLowerCase(),
        direccion: String(
            emailData?.Direccion_despacho ||
            getClientField(clientRecord, ['Dirección Despacho', 'DirecciÃ³n Despacho'])
        ).trim().toLowerCase(),
        ocNumber: String(emailData?.Orden_de_Compra || '').trim().toLowerCase(),
        requestedDeliveryDay: String(requestedDeliveryDay || '').trim(),
        sender: String(sender || emailData?.Sender_Email || '').trim().toLowerCase(),
        emailDate: String(emailDate || '').trim(),
        quantities
    };

    return createHash('sha256').update(JSON.stringify(payload)).digest('hex');
}

function buildReservationContext({ emailData, clientData, emailContext }) {
    const clientRecord = clientData && typeof clientData === 'object'
        ? (clientData.data || clientData)
        : {};
    const quantities = getOrderQuantities(emailData);
    const totalBoxes = getTotalBoxes(emailData);
    const senderEmail = String(emailData?.Sender_Email || emailContext?.sender || '').trim();

    return {
        senderEmail,
        emailSubject: String(emailContext?.emailSubject || '').trim(),
        emailDate: String(emailContext?.emailDate || '').trim(),
        source: String(emailContext?.source || '').trim(),
        attachmentFilename: String(emailContext?.attachmentFilename || '').trim(),
        order: {
            ocNumber: emailData?.Orden_de_Compra ?? null,
            totalBoxes,
            quantities
        },
        client: {
            rut: String(emailData?.Rut || clientRecord?.RUT || '').trim(),
            razonSocial: String(
                emailData?.Razon_social ||
                getClientField(clientRecord, ['RAZÓN SOCIAL', 'RAZÃ“N SOCIAL'])
            ).trim(),
            direccionDespacho: String(
                emailData?.Direccion_despacho ||
                getClientField(clientRecord, ['Dirección Despacho', 'DirecciÃ³n Despacho'])
            ).trim(),
            comuna: String(
                emailData?.Comuna ||
                getClientField(clientRecord, ['Comuna Despacho'])
            ).trim(),
            region: normalizeRegion(clientRecord?.region || '')
        },
        rawExtractedData: {
            emailData: toPlainObject(emailData),
            clientData: toPlainObject(clientRecord)
        }
    };
}

function findNextDeliveryDateForComuna(comuna, fromDate) {
    const normalizedComuna = normalizeComuna(comuna);
    if (!normalizedComuna) {
        return null;
    }

    const deliveryDayIndexes = getDeliveryDayIndexesByComuna(normalizedComuna);
    if (!deliveryDayIndexes.length) {
        return null;
    }

    const deliveryDaysSet = new Set(deliveryDayIndexes.map((day) => day.index));
    const baseDate = normalizeDate(fromDate);
    const baseMoment = baseDate
        ? moment.tz(baseDate, 'YYYY-MM-DD', CHILE_TIMEZONE)
        : getNowMoment().startOf('day');

    for (let offset = 1; offset <= 60; offset++) {
        const candidate = baseMoment.clone().add(offset, 'day');
        if (deliveryDaysSet.has(candidate.day())) {
            return candidate.format('YYYY-MM-DD');
        }
    }

    return null;
}

function isReservationExpired(reservation) {
    const expiresAt = reservation?.expiresAt;
    if (!expiresAt) {
        return false;
    }

    const expiresMoment = moment(expiresAt);
    if (!expiresMoment.isValid()) {
        return false;
    }

    return expiresMoment.isSameOrBefore(getNowMoment());
}

function isHoldReservationStillActive(reservation, referenceDate = new Date()) {
    if (!reservation || reservation.status !== HOLD_STATUS) {
        return false;
    }

    const expiresAt = reservation.expiresAt;
    if (!expiresAt) {
        return true;
    }

    const expiresMoment = moment(expiresAt);
    if (!expiresMoment.isValid()) {
        return true;
    }

    return expiresMoment.isAfter(moment(referenceDate));
}

function calculateEffectiveActivePoints(reservations, referenceDate = new Date()) {
    const source = Array.isArray(reservations) ? reservations : [];
    return source.reduce((acc, reservation) => {
        const points = toPositiveInteger(reservation?.points, ORDER_POINTS);
        const status = reservation?.status;
        if (status === COMMITTED_STATUS || status === BILLING_IN_PROGRESS_STATUS) {
            return acc + points;
        }
        if (isHoldReservationStillActive(reservation, referenceDate)) {
            return acc + points;
        }
        return acc;
    }, 0);
}

function getReservationId(input) {
    if (!input) {
        return null;
    }

    if (typeof input === 'string') {
        const value = input.trim();
        return value || null;
    }

    if (typeof input === 'object' && input.reservationId) {
        const value = String(input.reservationId).trim();
        return value || null;
    }

    return null;
}

function serializeReservation(record) {
    if (!record || !record.reservation) {
        return null;
    }

    const activePoints = Number(record.activePoints || 0);
    const maxPoints = Number(record.maxPoints || getDefaultMaxPoints());

    return {
        ...record.reservation,
        day: {
            deliveryDate: record.deliveryDate,
            activePoints,
            maxPoints,
            availablePoints: Math.max(maxPoints - activePoints, 0)
        }
    };
}

async function getMongoClient() {
    if (!MONGO_URI) {
        throw new Error('MONGO_URI no esta definido en .env');
    }

    if (!clientPromise) {
        const client = new MongoClient(MONGO_URI);
        clientPromise = client.connect();
    }

    return clientPromise;
}

async function getDaysCollection() {
    const client = await getMongoClient();
    const db = client.db(DB_NAME);
    const collection = db.collection(DAYS_COLLECTION_NAME);

    if (!daysIndexesReady) {
        await collection.createIndex({ region: 1, deliveryDate: 1 }, { unique: true });
        await collection.createIndex(
            { 'delivery_capacity_reservations.reservationId': 1 },
            { unique: true, sparse: true }
        );
        await collection.createIndex({
            'delivery_capacity_reservations.status': 1,
            'delivery_capacity_reservations.expiresAt': 1
        });
        daysIndexesReady = true;
    }

    return collection;
}

async function getConfigCollection() {
    const client = await getMongoClient();
    const db = client.db(DB_NAME);
    const collection = db.collection(CONFIG_COLLECTION_NAME);

    if (!configIndexesReady) {
        // _id is always unique by default in MongoDB; do not set unique explicitly.
        await collection.createIndex({ _id: 1 });
        configIndexesReady = true;
    }

    return collection;
}

async function getMaxPointsForRegion(region = RM_REGION_CODE, overrideValue = null) {
    const overrideAsNumber = Number(overrideValue);
    if (Number.isFinite(overrideAsNumber) && overrideAsNumber > 0) {
        return Math.floor(overrideAsNumber);
    }

    const collection = await getConfigCollection();
    const now = new Date();
    const docId = `GLOBAL_${normalizeRegion(region)}`;
    const defaultMaxPoints = getDefaultMaxPoints();

    const result = await collection.findOneAndUpdate(
        { _id: docId },
        {
            $setOnInsert: {
                _id: docId,
                region: normalizeRegion(region),
                maxPoints: defaultMaxPoints,
                createdAt: now
            },
            $set: {
                updatedAt: now
            }
        },
        {
            upsert: true,
            returnDocument: 'after'
        }
    );

    return toPositiveInteger(result?.maxPoints, defaultMaxPoints);
}

async function findDeliveryReservationById(reservationId) {
    const parsedReservationId = getReservationId(reservationId);
    if (!parsedReservationId) {
        return null;
    }

    const collection = await getDaysCollection();
    const doc = await collection.findOne(
        { 'delivery_capacity_reservations.reservationId': parsedReservationId },
        {
            projection: {
                region: 1,
                deliveryDate: 1,
                maxPoints: 1,
                activePoints: 1,
                delivery_capacity_reservations: {
                    $elemMatch: { reservationId: parsedReservationId }
                }
            }
        }
    );

    if (!doc || !Array.isArray(doc.delivery_capacity_reservations) || !doc.delivery_capacity_reservations[0]) {
        return null;
    }

    return {
        documentId: doc._id,
        region: doc.region,
        deliveryDate: doc.deliveryDate,
        maxPoints: toPositiveInteger(doc.maxPoints, getDefaultMaxPoints()),
        activePoints: toPositiveInteger(doc.activePoints, 0),
        reservation: doc.delivery_capacity_reservations[0]
    };
}

async function recalculateActivePointsForDay(deliveryDate, region = RM_REGION_CODE) {
    const collection = await getDaysCollection();
    const parsedDate = normalizeDate(deliveryDate);
    if (!parsedDate) {
        return {
            recalculated: false,
            reason: 'invalid_delivery_date'
        };
    }

    const normalizedRegion = normalizeRegion(region);
    const doc = await collection.findOne(
        { region: normalizedRegion, deliveryDate: parsedDate },
        {
            projection: {
                _id: 1,
                activePoints: 1,
                delivery_capacity_reservations: 1
            }
        }
    );

    if (!doc) {
        return {
            recalculated: false,
            reason: 'day_not_found'
        };
    }

    const now = new Date();
    const computedActivePoints = calculateEffectiveActivePoints(
        doc.delivery_capacity_reservations || [],
        now
    );
    const currentActivePoints = Number(doc.activePoints || 0);

    if (currentActivePoints === computedActivePoints) {
        return {
            recalculated: false,
            reason: 'already_consistent',
            deliveryDate: parsedDate,
            region: normalizedRegion,
            activePoints: currentActivePoints
        };
    }

    await collection.updateOne(
        { _id: doc._id },
        {
            $set: {
                activePoints: computedActivePoints,
                updatedAt: now
            }
        }
    );

    return {
        recalculated: true,
        deliveryDate: parsedDate,
        region: normalizedRegion,
        previousActivePoints: currentActivePoints,
        activePoints: computedActivePoints
    };
}

async function expireStaleHoldsForDay(deliveryDate, region = RM_REGION_CODE) {
    const collection = await getDaysCollection();
    const parsedDate = normalizeDate(deliveryDate);
    if (!parsedDate) {
        return 0;
    }

    const doc = await collection.findOne(
        { region: normalizeRegion(region), deliveryDate: parsedDate },
        {
            projection: {
                _id: 1,
                delivery_capacity_reservations: 1
            }
        }
    );

    if (!doc || !Array.isArray(doc.delivery_capacity_reservations) || doc.delivery_capacity_reservations.length === 0) {
        return 0;
    }

    const now = new Date();
    const staleReservations = doc.delivery_capacity_reservations.filter((reservation) => {
        if (reservation?.status !== HOLD_STATUS) {
            return false;
        }
        if (!reservation.expiresAt) {
            return false;
        }
        return moment(reservation.expiresAt).isSameOrBefore(moment(now));
    });

    if (!staleReservations.length) {
        return 0;
    }

    let expiredCount = 0;

    for (const staleReservation of staleReservations) {
        const points = toPositiveInteger(staleReservation?.points, ORDER_POINTS);
        const updateResult = await collection.updateOne(
            {
                _id: doc._id,
                delivery_capacity_reservations: {
                    $elemMatch: {
                        reservationId: staleReservation.reservationId,
                        status: HOLD_STATUS
                    }
                }
            },
            {
                $set: {
                    'delivery_capacity_reservations.$.status': EXPIRED_STATUS,
                    'delivery_capacity_reservations.$.updatedAt': now,
                    'delivery_capacity_reservations.$.expiredAt': now,
                    'delivery_capacity_reservations.$.expirationReason': 'hold_timeout',
                    updatedAt: now
                },
                $inc: {
                    activePoints: -points
                }
            }
        );

        if (updateResult.modifiedCount > 0) {
            expiredCount += 1;
        }
    }

    if (expiredCount > 0) {
        await recalculateActivePointsForDay(parsedDate, region);
    }

    return expiredCount;
}

async function expireStaleHoldsGlobally() {
    const collection = await getDaysCollection();
    const now = new Date();

    const daysWithExpiredHolds = await collection.find(
        {
            delivery_capacity_reservations: {
                $elemMatch: {
                    status: HOLD_STATUS,
                    expiresAt: { $lt: now }
                }
            }
        },
        {
            projection: {
                region: 1,
                deliveryDate: 1
            }
        }
    ).toArray();

    let expiredReservations = 0;
    let recalculatedDays = 0;

    for (const dayDoc of daysWithExpiredHolds) {
        const region = dayDoc?.region || RM_REGION_CODE;
        const deliveryDate = dayDoc?.deliveryDate || null;
        if (!deliveryDate) {
            continue;
        }

        const expiredInDay = await expireStaleHoldsForDay(deliveryDate, region);
        expiredReservations += Number(expiredInDay || 0);

        const recalc = await recalculateActivePointsForDay(deliveryDate, region);
        if (recalc?.recalculated) {
            recalculatedDays += 1;
        }
    }

    return {
        scannedDays: daysWithExpiredHolds.length,
        expiredReservations,
        recalculatedDays,
        executedAt: now.toISOString()
    };
}

function startDeliveryCapacityCleanupCron({
    intervalMinutes = 12,
    runOnStart = true,
    logger = console
} = {}) {
    const safeIntervalMinutes = toPositiveInteger(intervalMinutes, 12);
    const intervalMs = safeIntervalMinutes * 60 * 1000;
    let isRunning = false;

    const runCycle = async () => {
        if (isRunning) {
            return { skipped: true, reason: 'cycle_already_running' };
        }

        isRunning = true;
        try {
            const summary = await expireStaleHoldsGlobally();
            if (summary.expiredReservations > 0 || summary.recalculatedDays > 0) {
                logger.log(
                    `[DeliveryCapacityCron] Expired ${summary.expiredReservations} holds across ${summary.scannedDays} day docs`
                );
            }
            return summary;
        } catch (error) {
            logger.error('[DeliveryCapacityCron] Failed to clean stale holds:', error);
            return {
                error: error?.message || String(error)
            };
        } finally {
            isRunning = false;
        }
    };

    if (runOnStart) {
        runCycle().catch(() => { });
    }

    const timer = setInterval(() => {
        runCycle().catch(() => { });
    }, intervalMs);

    if (typeof timer.unref === 'function') {
        timer.unref();
    }

    return {
        intervalMs,
        stop() {
            clearInterval(timer);
        },
        runNow: runCycle
    };
}

async function insertReservationOnDay({
    reservation,
    deliveryDate,
    maxPoints,
    region = RM_REGION_CODE
}) {
    const collection = await getDaysCollection();
    const parsedDate = normalizeDate(deliveryDate);
    if (!parsedDate) {
        return { inserted: false, reason: 'invalid_delivery_date' };
    }

    const normalizedRegion = normalizeRegion(region);
    const pointsToReserve = toPositiveInteger(reservation?.points, ORDER_POINTS);
    const now = new Date();

    // 1) Ensure the day document exists. This avoids duplicate-key errors when the day is full.
    try {
        await collection.updateOne(
            {
                region: normalizedRegion,
                deliveryDate: parsedDate
            },
            {
                $setOnInsert: {
                    region: normalizedRegion,
                    deliveryDate: parsedDate,
                    activePoints: 0,
                    delivery_capacity_reservations: [],
                    createdAt: now
                },
                $set: {
                    maxPoints,
                    updatedAt: now
                }
            },
            {
                upsert: true
            }
        );
    } catch (error) {
        if (error?.code !== 11000) {
            throw error;
        }
        // Another concurrent request created it first; continue.
    }

    // 2) Try to reserve capacity only if there is space available.
    const reserveResult = await collection.updateOne(
        {
            region: normalizedRegion,
            deliveryDate: parsedDate,
            activePoints: { $lt: maxPoints }
        },
        {
            $set: {
                maxPoints,
                updatedAt: now
            },
            $push: {
                delivery_capacity_reservations: reservation
            },
            $inc: {
                activePoints: pointsToReserve
            }
        }
    );

    if (reserveResult.modifiedCount === 0) {
        return { inserted: false, reason: 'capacity_full' };
    }

    const savedReservation = await findDeliveryReservationById(reservation.reservationId);
    return {
        inserted: true,
        reservation: savedReservation
    };
}

async function reserveNextAvailableDay({
    reservation,
    preferredDeliveryDay,
    comuna,
    maxPoints,
    region = RM_REGION_CODE
}) {
    let candidateDate = normalizeDate(preferredDeliveryDay);
    if (!candidateDate) {
        return { inserted: false, reason: 'invalid_preferred_delivery_day' };
    }

    for (let attempts = 0; attempts < 90; attempts++) {
        await expireStaleHoldsForDay(candidateDate, region);

        const reservationToInsert = {
            ...reservation,
            assignedDeliveryDay: candidateDate
        };

        const insertResult = await insertReservationOnDay({
            reservation: reservationToInsert,
            deliveryDate: candidateDate,
            maxPoints,
            region
        });

        if (insertResult.inserted) {
            return insertResult;
        }

        candidateDate = findNextDeliveryDateForComuna(comuna, candidateDate);
        if (!candidateDate) {
            break;
        }
    }

    return { inserted: false, reason: 'no_capacity_available' };
}

async function updateReservationStatus({
    reservationId,
    allowedCurrentStatuses = [],
    nextStatus,
    releasePoints = false,
    extraFields = {}
}) {
    const parsedReservationId = getReservationId(reservationId);
    if (!parsedReservationId) {
        return null;
    }

    const record = await findDeliveryReservationById(parsedReservationId);
    if (!record) {
        return null;
    }

    const currentStatus = record.reservation?.status;
    if (Array.isArray(allowedCurrentStatuses) && allowedCurrentStatuses.length > 0) {
        if (!allowedCurrentStatuses.includes(currentStatus)) {
            return record;
        }
    }

    const collection = await getDaysCollection();
    const now = new Date();
    const setPayload = {
        'delivery_capacity_reservations.$.status': nextStatus,
        'delivery_capacity_reservations.$.updatedAt': now,
        updatedAt: now
    };

    Object.entries(extraFields || {}).forEach(([key, value]) => {
        setPayload[`delivery_capacity_reservations.$.${key}`] = value;
    });

    const shouldReleasePoints =
        releasePoints &&
        ACTIVE_STATUSES.has(currentStatus) &&
        !ACTIVE_STATUSES.has(nextStatus);

    const updatePayload = { $set: setPayload };
    if (shouldReleasePoints) {
        updatePayload.$inc = {
            activePoints: -toPositiveInteger(record.reservation?.points, ORDER_POINTS)
        };
    }

    await collection.updateOne(
        {
            _id: record.documentId,
            delivery_capacity_reservations: {
                $elemMatch: {
                    reservationId: parsedReservationId,
                    status: currentStatus
                }
            }
        },
        updatePayload
    );

    if (shouldReleasePoints) {
        await collection.updateOne(
            { _id: record.documentId, activePoints: { $lt: 0 } },
            { $set: { activePoints: 0, updatedAt: now } }
        );
    }

    return findDeliveryReservationById(parsedReservationId);
}

function buildReservationTemplate({
    emailData,
    clientData,
    requestedDeliveryDay,
    emailContext,
    revivedFromReservationId = null
}) {
    const clientRecord = clientData && typeof clientData === 'object'
        ? (clientData.data || clientData)
        : {};
    const normalizedRequestedDay = normalizeDate(requestedDeliveryDay);
    const ttlMinutes = getHoldTtlMinutes();
    const now = new Date();
    const expiresAt = moment(now).add(ttlMinutes, 'minutes').toDate();
    const senderEmail = String(emailData?.Sender_Email || emailContext?.sender || '').trim();
    const fingerprintHash = buildReservationFingerprint({
        emailData,
        clientRecord,
        requestedDeliveryDay: normalizedRequestedDay,
        sender: senderEmail,
        emailDate: emailContext?.emailDate
    });

    return {
        reservationId: randomUUID(),
        status: HOLD_STATUS,
        points: ORDER_POINTS,
        requestedDeliveryDay: normalizedRequestedDay,
        assignedDeliveryDay: normalizedRequestedDay,
        region: RM_REGION_CODE,
        comuna: normalizeComuna(
            getClientField(clientRecord, ['Comuna Despacho']) || emailData?.Comuna || ''
        ),
        ocNumber: emailData?.Orden_de_Compra ?? null,
        clientRut: String(emailData?.Rut || clientRecord?.RUT || '').trim(),
        senderEmail,
        fingerprintHash,
        createdAt: now,
        updatedAt: now,
        expiresAt,
        revivedFromReservationId,
        context: buildReservationContext({ emailData, clientData, emailContext })
    };
}

function shouldReserveInRm({ emailData, clientData }) {
    const clientRecord = clientData && typeof clientData === 'object'
        ? (clientData.data || clientData)
        : {};
    const region = normalizeRegion(clientRecord?.region || '');
    const isDelivery = emailData?.isDelivery !== false && emailData?.isDelivery !== 'false';
    const deliveryDay = normalizeDate(clientRecord?.deliveryDay);
    const normalizedComuna = normalizeComuna(
        getClientField(clientRecord, ['Comuna Despacho']) || emailData?.Comuna || ''
    );

    if (region !== RM_REGION_CODE) {
        return { reserve: false, reason: 'non_rm_region' };
    }
    if (!isDelivery) {
        return { reserve: false, reason: 'not_a_delivery_order' };
    }
    if (!deliveryDay) {
        return { reserve: false, reason: 'missing_delivery_day' };
    }
    if (!normalizedComuna) {
        return { reserve: false, reason: 'missing_comuna' };
    }

    return {
        reserve: true,
        region,
        comuna: normalizedComuna,
        requestedDeliveryDay: deliveryDay
    };
}

async function createDeliveryReservationForAnalysis({
    emailData,
    clientData,
    emailContext = {},
    maxPointsOverride = null
}) {
    const decision = shouldReserveInRm({ emailData, clientData });
    if (!decision.reserve) {
        return {
            success: true,
            skipped: true,
            reason: decision.reason,
            deliveryReservation: null
        };
    }

    const maxPoints = await getMaxPointsForRegion(RM_REGION_CODE, maxPointsOverride);
    const reservationTemplate = buildReservationTemplate({
        emailData,
        clientData,
        requestedDeliveryDay: decision.requestedDeliveryDay,
        emailContext
    });

    const reserveResult = await reserveNextAvailableDay({
        reservation: reservationTemplate,
        preferredDeliveryDay: decision.requestedDeliveryDay,
        comuna: decision.comuna,
        maxPoints,
        region: RM_REGION_CODE
    });

    if (!reserveResult.inserted || !reserveResult.reservation) {
        throw new Error(`No fue posible reservar cupo de despacho (${reserveResult.reason || 'unknown_error'})`);
    }

    return {
        success: true,
        skipped: false,
        reason: null,
        deliveryReservation: serializeReservation(reserveResult.reservation)
    };
}

async function reviveReservationFromExpiredRecord(expiredRecord) {
    const currentReservation = expiredRecord?.reservation;
    if (!currentReservation) {
        throw new Error('No existe una reserva previa para revivir');
    }

    const rawEmailData = toPlainObject(currentReservation?.context?.rawExtractedData?.emailData || {});
    const rawClientData = toPlainObject(currentReservation?.context?.rawExtractedData?.clientData || {});
    const emailContext = {
        emailSubject: currentReservation?.context?.emailSubject || '',
        emailDate: currentReservation?.context?.emailDate || '',
        sender: currentReservation?.senderEmail || currentReservation?.context?.senderEmail || '',
        source: currentReservation?.context?.source || '',
        attachmentFilename: currentReservation?.context?.attachmentFilename || ''
    };

    const normalizedComuna = normalizeComuna(
        currentReservation.comuna ||
        getClientField(rawClientData, ['Comuna Despacho']) ||
        rawEmailData?.Comuna ||
        ''
    );

    const preferredRevivalDay = normalizeDate(
        currentReservation.assignedDeliveryDay ||
        expiredRecord.deliveryDate ||
        currentReservation.requestedDeliveryDay
    );

    if (!preferredRevivalDay) {
        throw new Error('No se pudo determinar fecha base para revivir reserva');
    }

    const maxPoints = await getMaxPointsForRegion(RM_REGION_CODE, null);
    const reservationTemplate = buildReservationTemplate({
        emailData: rawEmailData,
        clientData: { data: rawClientData },
        requestedDeliveryDay: preferredRevivalDay,
        emailContext,
        revivedFromReservationId: currentReservation.reservationId
    });

    const reserveResult = await reserveNextAvailableDay({
        reservation: reservationTemplate,
        preferredDeliveryDay: preferredRevivalDay,
        comuna: normalizedComuna,
        maxPoints,
        region: RM_REGION_CODE
    });

    if (!reserveResult.inserted || !reserveResult.reservation) {
        throw new Error(`No se pudo revivir reserva vencida (${reserveResult.reason || 'unknown_error'})`);
    }

    return serializeReservation(reserveResult.reservation);
}

async function prepareDeliveryReservationForBilling({ deliveryReservation }) {
    const reservationId = getReservationId(deliveryReservation);
    if (!reservationId) {
        return {
            success: true,
            skipped: true,
            reason: 'missing_delivery_reservation',
            deliveryReservation: null
        };
    }

    const record = await findDeliveryReservationById(reservationId);
    if (!record) {
        throw {
            code: 400,
            error: 'Bad request',
            message: `deliveryReservation no existe (${reservationId})`
        };
    }

    let currentRecord = record;
    let currentReservation = currentRecord.reservation;

    if (currentReservation.status === HOLD_STATUS && isReservationExpired(currentReservation)) {
        await updateReservationStatus({
            reservationId: currentReservation.reservationId,
            allowedCurrentStatuses: [HOLD_STATUS],
            nextStatus: EXPIRED_STATUS,
            releasePoints: true,
            extraFields: {
                expiredAt: new Date(),
                expirationReason: 'expired_before_billing'
            }
        });

        await recalculateActivePointsForDay(
            currentRecord.deliveryDate,
            currentRecord.region || RM_REGION_CODE
        );

        const refreshedRecord = await findDeliveryReservationById(currentReservation.reservationId);
        const baseExpiredRecord = refreshedRecord || currentRecord;
        const revivedReservation = await reviveReservationFromExpiredRecord(baseExpiredRecord);

        currentRecord = await findDeliveryReservationById(revivedReservation.reservationId);
        currentReservation = currentRecord?.reservation;
    } else if (currentReservation.status === EXPIRED_STATUS) {
        await recalculateActivePointsForDay(
            currentRecord.deliveryDate,
            currentRecord.region || RM_REGION_CODE
        );

        const revivedReservation = await reviveReservationFromExpiredRecord(currentRecord);
        currentRecord = await findDeliveryReservationById(revivedReservation.reservationId);
        currentReservation = currentRecord?.reservation;
    }

    if (!currentRecord || !currentReservation) {
        throw new Error('No se pudo preparar la reserva de despacho para facturacion');
    }

    if (currentReservation.status === HOLD_STATUS) {
        const updatedRecord = await updateReservationStatus({
            reservationId: currentReservation.reservationId,
            allowedCurrentStatuses: [HOLD_STATUS],
            nextStatus: BILLING_IN_PROGRESS_STATUS,
            releasePoints: false,
            extraFields: {
                billingStartedAt: new Date()
            }
        });
        currentRecord = updatedRecord || currentRecord;
    }

    return {
        success: true,
        skipped: false,
        reason: null,
        deliveryReservation: serializeReservation(currentRecord)
    };
}

async function markDeliveryReservationAsCommitted({ deliveryReservation, defontanaResponse = null }) {
    const reservationId = getReservationId(deliveryReservation);
    if (!reservationId) {
        return { success: true, skipped: true, reason: 'missing_delivery_reservation', deliveryReservation: null };
    }

    const updatedRecord = await updateReservationStatus({
        reservationId,
        allowedCurrentStatuses: [BILLING_IN_PROGRESS_STATUS, HOLD_STATUS],
        nextStatus: COMMITTED_STATUS,
        releasePoints: false,
        extraFields: {
            committedAt: new Date(),
            defontanaSummary: toPlainObject(defontanaResponse || {})
        }
    });

    return {
        success: true,
        skipped: false,
        reason: null,
        deliveryReservation: serializeReservation(updatedRecord)
    };
}

async function markDeliveryReservationAsFailed({
    deliveryReservation,
    reason = 'billing_failed',
    defontanaResponse = null
}) {
    const reservationId = getReservationId(deliveryReservation);
    if (!reservationId) {
        return { success: true, skipped: true, reason: 'missing_delivery_reservation', deliveryReservation: null };
    }

    const updatedRecord = await updateReservationStatus({
        reservationId,
        allowedCurrentStatuses: [BILLING_IN_PROGRESS_STATUS, HOLD_STATUS],
        nextStatus: FAILED_STATUS,
        releasePoints: true,
        extraFields: {
            failedAt: new Date(),
            failureReason: String(reason || 'billing_failed'),
            defontanaSummary: toPlainObject(defontanaResponse || {})
        }
    });

    return {
        success: true,
        skipped: false,
        reason: null,
        deliveryReservation: serializeReservation(updatedRecord)
    };
}

export {
    createDeliveryReservationForAnalysis,
    prepareDeliveryReservationForBilling,
    markDeliveryReservationAsCommitted,
    markDeliveryReservationAsFailed,
    expireStaleHoldsGlobally,
    startDeliveryCapacityCleanupCron,
    HOLD_STATUS,
    BILLING_IN_PROGRESS_STATUS,
    COMMITTED_STATUS,
    FAILED_STATUS,
    EXPIRED_STATUS
};
