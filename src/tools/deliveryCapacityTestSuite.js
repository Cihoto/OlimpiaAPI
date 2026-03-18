import dotenv from 'dotenv';
import { MongoClient } from 'mongodb';

dotenv.config();

const MONGO_URI = process.env.MONGO_URI;
if (!MONGO_URI) {
    console.error('MONGO_URI no esta definido. No se puede ejecutar la suite.');
    process.exit(1);
}

const DAYS_COLLECTION_NAME = process.env.MONGO_DELIVERY_CAPACITY_DAYS_COLLECTION || 'delivery_capacity_days';
const CONFIG_COLLECTION_NAME = process.env.MONGO_DELIVERY_CAPACITY_CONFIG_COLLECTION || 'delivery_capacity_config';
const SUITE_DB_NAME = `Olimpia_DeliveryCapacitySuite_${Date.now()}`;

process.env.MONGO_DB_NAME = SUITE_DB_NAME;
process.env.MONGO_DELIVERY_CAPACITY_DAYS_COLLECTION = DAYS_COLLECTION_NAME;
process.env.MONGO_DELIVERY_CAPACITY_CONFIG_COLLECTION = CONFIG_COLLECTION_NAME;
process.env.DELIVERY_RM_MAX_POINTS_DEFAULT = process.env.DELIVERY_RM_MAX_POINTS_DEFAULT || '30';
process.env.DELIVERY_RESERVATION_HOLD_TTL_MINUTES = process.env.DELIVERY_RESERVATION_HOLD_TTL_MINUTES || '12';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function buildOrderSeed(seed, deliveryDay) {
    const rut = `77.108.${String(100 + seed).padStart(3, '0')}-${(seed % 9) + 1}`;
    return {
        emailData: {
            Razon_social: `Cliente Test ${seed}`,
            Direccion_despacho: `direccion test ${seed}`,
            Comuna: 'Lo Barnechea',
            Rut: rut,
            Orden_de_Compra: `OC-TEST-${seed}`,
            Sender_Email: `seed-${seed}@test.local`,
            isDelivery: true,
            Pedido_Cantidad_Pink: 0,
            Pedido_Cantidad_Amargo: 1,
            Pedido_Cantidad_Leche: 0,
            Pedido_Cantidad_Free: 0,
            Pedido_Cantidad_Pink_90g: 0,
            Pedido_Cantidad_Amargo_90g: 0,
            Pedido_Cantidad_Leche_90g: 0
        },
        clientData: {
            data: {
                RUT: rut,
                'Comuna Despacho': 'Lo Barnechea',
                deliveryDay,
                region: 'RM'
            }
        },
        emailContext: {
            emailSubject: `Suite Test ${seed}`,
            emailDate: new Date().toISOString(),
            sender: `seed-${seed}@test.local`,
            source: 'delivery_capacity_test_suite',
            attachmentFilename: ''
        }
    };
}

function summarizeStatuses(list) {
    return (list || []).reduce((acc, item) => {
        const key = String(item?.status || 'UNKNOWN');
        acc[key] = (acc[key] || 0) + 1;
        return acc;
    }, {});
}

function assertCondition(condition, message) {
    if (!condition) {
        throw new Error(message);
    }
}

async function getDayDoc(daysCollection, deliveryDate) {
    return daysCollection.findOne({ region: 'RM', deliveryDate });
}

async function run() {
    const mongoClient = new MongoClient(MONGO_URI);
    await mongoClient.connect();
    const db = mongoClient.db(SUITE_DB_NAME);
    const daysCollection = db.collection(DAYS_COLLECTION_NAME);
    const configCollection = db.collection(CONFIG_COLLECTION_NAME);

    const service = await import('../services/deliveryCapacityService.js');
    const {
        createDeliveryReservationForAnalysis,
        prepareDeliveryReservationForBilling,
        markDeliveryReservationAsCommitted,
        markDeliveryReservationAsFailed,
        expireStaleHoldsGlobally,
        startDeliveryCapacityCleanupCron
    } = service;

    const results = [];
    let failures = 0;

    const runScenario = async (name, fn) => {
        const startedAt = new Date();
        try {
            const details = await fn();
            results.push({
                name,
                status: 'PASS',
                startedAt: startedAt.toISOString(),
                finishedAt: new Date().toISOString(),
                details
            });
        } catch (error) {
            failures += 1;
            results.push({
                name,
                status: 'FAIL',
                startedAt: startedAt.toISOString(),
                finishedAt: new Date().toISOString(),
                error: error?.message || String(error)
            });
        }
    };

    await runScenario('A - Reserva RM crea HOLD y suma cupo', async () => {
        const payload = buildOrderSeed(1, '2026-03-18');
        const created = await createDeliveryReservationForAnalysis(payload);
        assertCondition(created.success === true && created.skipped === false, 'Reserva RM no fue creada');
        assertCondition(created.deliveryReservation?.status === 'HOLD', 'Estado inicial no es HOLD');
        assertCondition(created.deliveryReservation?.assignedDeliveryDay === '2026-03-18', 'Fecha asignada inesperada');

        const dayDoc = await getDayDoc(daysCollection, '2026-03-18');
        assertCondition(Number(dayDoc?.activePoints || 0) === 1, 'activePoints esperado en 1');
        return {
            reservationId: created.deliveryReservation.reservationId,
            dayActivePoints: dayDoc.activePoints
        };
    });

    await runScenario('B - Pedido fuera RM se omite', async () => {
        const payload = buildOrderSeed(2, '2026-03-18');
        payload.clientData.data.region = 'VI';
        const created = await createDeliveryReservationForAnalysis(payload);
        assertCondition(created.success === true && created.skipped === true, 'Pedido no RM no fue omitido');
        return { reason: created.reason };
    });

    await runScenario('C - Con dia lleno salta a siguiente fecha valida', async () => {
        const firstPayload = buildOrderSeed(3, '2026-03-25');
        const first = await createDeliveryReservationForAnalysis({
            ...firstPayload,
            maxPointsOverride: 1
        });
        assertCondition(first.deliveryReservation?.assignedDeliveryDay === '2026-03-25', 'Primera reserva no quedo en 25');

        const secondPayload = buildOrderSeed(4, '2026-03-25');
        const second = await createDeliveryReservationForAnalysis({
            ...secondPayload,
            maxPointsOverride: 1
        });
        assertCondition(second.deliveryReservation?.assignedDeliveryDay === '2026-03-27', 'No salto a 27 al llenarse 25');
        return {
            firstDay: first.deliveryReservation.assignedDeliveryDay,
            secondDay: second.deliveryReservation.assignedDeliveryDay
        };
    });

    await runScenario('D - Facturacion: HOLD -> BILLING_IN_PROGRESS -> COMMITTED', async () => {
        const payload = buildOrderSeed(5, '2026-03-30');
        const created = await createDeliveryReservationForAnalysis(payload);
        const reservationId = created.deliveryReservation.reservationId;

        const prepared = await prepareDeliveryReservationForBilling({ deliveryReservation: reservationId });
        assertCondition(prepared.deliveryReservation?.status === 'BILLING_IN_PROGRESS', 'No paso a BILLING_IN_PROGRESS');

        const committed = await markDeliveryReservationAsCommitted({
            deliveryReservation: prepared.deliveryReservation,
            defontanaResponse: { success: true }
        });
        assertCondition(committed.deliveryReservation?.status === 'COMMITTED', 'No paso a COMMITTED');
        return {
            reservationId,
            finalStatus: committed.deliveryReservation.status
        };
    });

    await runScenario('E - Facturacion fallida libera cupo', async () => {
        const payload = buildOrderSeed(6, '2026-04-01');
        const created = await createDeliveryReservationForAnalysis(payload);
        const reservationId = created.deliveryReservation.reservationId;
        const prepared = await prepareDeliveryReservationForBilling({ deliveryReservation: reservationId });
        const failed = await markDeliveryReservationAsFailed({
            deliveryReservation: prepared.deliveryReservation,
            reason: 'suite_failure_test',
            defontanaResponse: { success: false, error: true }
        });
        assertCondition(failed.deliveryReservation?.status === 'FAILED', 'No paso a FAILED');
        const dayDoc = await getDayDoc(daysCollection, '2026-04-01');
        assertCondition(Number(dayDoc?.activePoints || 0) === 0, 'No libero cupo tras FAILED');
        return {
            reservationId,
            dayActivePoints: dayDoc.activePoints
        };
    });

    await runScenario('F - Reserva vencida revive al facturar y recalcula cupo', async () => {
        const payload = buildOrderSeed(7, '2026-04-03');
        const created = await createDeliveryReservationForAnalysis(payload);
        const originalReservationId = created.deliveryReservation.reservationId;

        await daysCollection.updateOne(
            { 'delivery_capacity_reservations.reservationId': originalReservationId },
            {
                $set: {
                    'delivery_capacity_reservations.$.expiresAt': new Date(Date.now() - 2 * 60 * 1000),
                    'delivery_capacity_reservations.$.updatedAt': new Date()
                }
            }
        );

        const revived = await prepareDeliveryReservationForBilling({ deliveryReservation: originalReservationId });
        const revivedReservation = revived.deliveryReservation;
        assertCondition(revivedReservation?.reservationId !== originalReservationId, 'No genero nueva reserva al revivir');
        assertCondition(revivedReservation?.revivedFromReservationId === originalReservationId, 'No guardo referencia de revive');
        assertCondition(revivedReservation?.status === 'BILLING_IN_PROGRESS', 'Reserva revivida no quedo en BILLING_IN_PROGRESS');

        const oldDoc = await daysCollection.findOne(
            { 'delivery_capacity_reservations.reservationId': originalReservationId },
            { projection: { deliveryDate: 1, activePoints: 1, delivery_capacity_reservations: { $elemMatch: { reservationId: originalReservationId } } } }
        );
        assertCondition(oldDoc?.delivery_capacity_reservations?.[0]?.status === 'EXPIRED', 'Reserva original no quedo en EXPIRED');
        assertCondition(
            revivedReservation.assignedDeliveryDay === oldDoc.deliveryDate,
            'Reserva revivida no priorizo la misma fecha original'
        );
        return {
            originalReservationId,
            revivedReservationId: revivedReservation.reservationId,
            oldDay: oldDoc.deliveryDate,
            newDay: revivedReservation.assignedDeliveryDay
        };
    });

    await runScenario('I - Si ya esta EXPIRED al facturar, tambien revive', async () => {
        const payload = buildOrderSeed(10, '2026-04-10');
        const created = await createDeliveryReservationForAnalysis(payload);
        const originalReservationId = created.deliveryReservation.reservationId;

        await daysCollection.updateOne(
            { 'delivery_capacity_reservations.reservationId': originalReservationId },
            {
                $set: {
                    'delivery_capacity_reservations.$.expiresAt': new Date(Date.now() - 2 * 60 * 1000),
                    'delivery_capacity_reservations.$.updatedAt': new Date()
                }
            }
        );

        await expireStaleHoldsGlobally();

        const oldDoc = await daysCollection.findOne(
            { 'delivery_capacity_reservations.reservationId': originalReservationId },
            { projection: { deliveryDate: 1, activePoints: 1, delivery_capacity_reservations: { $elemMatch: { reservationId: originalReservationId } } } }
        );
        assertCondition(oldDoc?.delivery_capacity_reservations?.[0]?.status === 'EXPIRED', 'La reserva no quedo EXPIRED antes de facturar');

        const prepared = await prepareDeliveryReservationForBilling({ deliveryReservation: originalReservationId });
        const revived = prepared.deliveryReservation;
        assertCondition(revived?.reservationId !== originalReservationId, 'No creo nueva reserva desde EXPIRED');
        assertCondition(revived?.status === 'BILLING_IN_PROGRESS', 'No quedo en BILLING_IN_PROGRESS');
        assertCondition(revived?.revivedFromReservationId === originalReservationId, 'No trazo revivedFromReservationId');
        return {
            originalReservationId,
            revivedReservationId: revived.reservationId,
            oldDay: oldDoc.deliveryDate,
            newDay: revived.assignedDeliveryDay
        };
    });

    await runScenario('G - Limpieza global expira HOLD vencidos y recalcula cupo', async () => {
        const payloadA = buildOrderSeed(8, '2026-04-08');
        const payloadB = buildOrderSeed(9, '2026-04-08');
        const resA = await createDeliveryReservationForAnalysis(payloadA);
        const resB = await createDeliveryReservationForAnalysis(payloadB);

        await daysCollection.updateOne(
            { 'delivery_capacity_reservations.reservationId': resA.deliveryReservation.reservationId },
            { $set: { 'delivery_capacity_reservations.$.expiresAt': new Date(Date.now() - 2 * 60 * 1000) } }
        );

        const summary = await expireStaleHoldsGlobally();
        assertCondition(Number(summary.expiredReservations || 0) >= 1, 'No expiro HOLD vencidos');

        const dayDoc = await getDayDoc(daysCollection, '2026-04-08');
        const statuses = summarizeStatuses(dayDoc?.delivery_capacity_reservations || []);
        assertCondition(statuses.EXPIRED >= 1, 'No hay reservas EXPIRED tras limpieza');
        assertCondition(statuses.HOLD >= 1, 'No mantuvo HOLD vigente');
        assertCondition(Number(dayDoc?.activePoints || 0) === 1, 'activePoints no recalculado tras limpieza');
        return {
            cleanupSummary: summary,
            dayStatusCounts: statuses,
            dayActivePoints: dayDoc.activePoints
        };
    });

    await runScenario('H - Cron expone runNow y stop', async () => {
        const cron = startDeliveryCapacityCleanupCron({
            intervalMinutes: 12,
            runOnStart: false,
            logger: console
        });
        await sleep(100);
        const runNow = await cron.runNow();
        cron.stop();
        assertCondition(runNow && typeof runNow === 'object', 'runNow no devolvio resumen');
        return { runNow };
    });

    const dbSummary = await daysCollection.find(
        {},
        {
            projection: {
                deliveryDate: 1,
                region: 1,
                activePoints: 1,
                maxPoints: 1,
                delivery_capacity_reservations: { status: 1, points: 1, expiresAt: 1 }
            }
        }
    ).toArray();

    const finalSummary = dbSummary.map((doc) => ({
        deliveryDate: doc.deliveryDate,
        region: doc.region,
        activePoints: doc.activePoints,
        maxPoints: doc.maxPoints,
        statusCounts: summarizeStatuses(doc.delivery_capacity_reservations || [])
    }));

    console.log(JSON.stringify({
        suiteDbName: SUITE_DB_NAME,
        scenarios: results,
        totals: {
            total: results.length,
            passed: results.filter((item) => item.status === 'PASS').length,
            failed: failures
        },
        finalSummary
    }, null, 2));

    await db.dropDatabase();
    await mongoClient.close();

    process.exit(failures > 0 ? 1 : 0);
}

run().catch(async (error) => {
    console.error('Suite failed with fatal error:', error);
    process.exit(1);
});
