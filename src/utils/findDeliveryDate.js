// const moment = require('moment'); // Import moment.js for date manipulation
// require('moment-timezone'); // Import moment-timezone for timezone support
// moment.tz.setDefault('America/Santiago'); // Set default timezone to Chile's timezone
// import moment from 'moment'; // Import moment.js for date manipulation



import moment from 'moment-timezone'; // Import moment-timezone for timezone support
import { isFileLike } from 'openai/uploads.mjs';
// moment.tz.setDefault('America/Santiago'); // Set default timezone to Chile's timezone


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
    "MACUL"
]

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
            "QUILICURA"
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
            "HUECHURABA"
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
        ]
    }
];

function findDeliveryDayByComuna(comunaToSearch, emailDate) {

    try {
        // const todayWeekDayIndex = moment().day(); // Obtiene el índice del día de la semana (0 para domingo, 1 para lunes, etc.)
        // Obtiene el índice del día de la semana de la fecha del correo electrónico
        // Obtiene la hora de la fecha del correo electrónico
        // const formattedDate = moment(emailDate).format("YYYY/MM/DD HH:mm:ss");

        if (!comunaToSearch || typeof comunaToSearch !== 'string') {
            return null; // Invalid input
        }
        console.log("comunaToSearch 2", comunaToSearch);


        // Check if the comunaToSearch is in the list of unique communities
        const isValidCommunity = uniqueCommunities.find(community => {
            const normalize = (str) => str.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().trim();
            return normalize(community) === normalize(comunaToSearch);
        });

        if (!isValidCommunity) {
            return null; // Invalid community
        }

        // Find all indexes of the delivery days that match the comunaToSearch
        const deliveryDayIndexes = deliveryDays
            .filter(day => day.communities.some(community => {
                const normalize = (str) => str.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().trim();
                return normalize(community) === normalize(comunaToSearch);
            }))
            .map(day => {
                return {
                    index: day.index,
                    dayName: day.dayName
                }
            });

        deliveryDayIndexes.sort((a, b) => a.index - b.index); // Sort by index
        console.log("deliveryDayIndexes", deliveryDayIndexes);
        let deliveryIndex = null;

        const emailDateDayIndex = moment(emailDate, 'YYYY-MM-DDTHH:mm:ss.SSSZ').day();
        // Solo obtener la hora local de la fecha sin convertir a otra zona horaria
        // Obtener la hora en horario chileno (America/Santiago)
        const emailDateHour = moment.tz(emailDate, 'YYYY-MM-DDTHH:mm:ss.SSSZ', 'America/Santiago').hour();
        const emailDateFormatted = moment(emailDate).format("YYYY-MM-DD");

        //encontrar el proximo indice de entrega
        for (let i = 0; i < deliveryDayIndexes.length; i++) {
            // console.log("deliveryDayIndexes", deliveryDayIndexes[i]);
            const deliveryDayIndex = deliveryDayIndexes[i].index;

            if (emailDateDayIndex == 6 || emailDateDayIndex == 0) {

                // deliveryIndex = DeliveryDaySelector(deliveryDayIndexes, i, emailDateFormatted,emailDateHour)
                deliveryIndex = moveForward(deliveryDayIndexes.length, i, 1)
                break;
            }

            if (emailDateDayIndex == 5) {
                console.log("________________________________________________________________________________")

                deliveryIndex = DeliveryDaySelector(deliveryDayIndexes, i, emailDateFormatted, emailDateHour)
                // break;
                const daysToNextDelivery = diffToNextDeliveryDay(deliveryDayIndexes, i, emailDateFormatted);

                // deliveryIndex = daysToNextDelivery;
                // break;
                // if (daysToNextDelivery > 1) {
                //     deliveryIndex = moveForward(deliveryDayIndexes.length, i, 0)
                //     break;
                // }
                if (emailDateHour >= 12) {
                    deliveryIndex = moveForward(deliveryDayIndexes.length, i, 1)
                } else {
                    deliveryIndex = moveForward(deliveryDayIndexes.length, i, 0)
                }
                break;
            }

            if (deliveryDayIndex > emailDateDayIndex) {

                console.log("________________________________SEPARATOR_______________________________________________")

                deliveryIndex = DeliveryDaySelector(deliveryDayIndexes, i, emailDateFormatted, emailDateHour)

                const daysToNextDelivery = diffToNextDeliveryDay(deliveryDayIndexes, i, emailDateFormatted);
                console.log("daysToNextDelivery", daysToNextDelivery)

                if (daysToNextDelivery > 1) {
                    deliveryIndex = moveForward(deliveryDayIndexes.length, i, 0)
                    break;
                }

                if (emailDateHour >= 12) {
                    console.log("laskdejalsdkjalskdjalsd", emailDateHour)
                    deliveryIndex = moveForward(deliveryDayIndexes.length, i, 1)
                } else {
                    console.log({ emailDateHour })
                    deliveryIndex = moveForward(deliveryDayIndexes.length, i, 0)
                }
                break;
            }


        }

        // Get the day name using the deliveryIndex
        const deliveryObj = deliveryDayIndexes[deliveryIndex]

        // teniendo en cuenta deliveryObject.dayName que puede ser LUNES, MARTES, MIERCOLES, JUEVES, VIERNES, buscar la fecha futura mas cercana que sea igual a la fecha de entrega teniendo como punto de partida la fecha del correo
        let deliveryDate = null;
        let date = emailDate;
        // while (deliveryObj.index != deliveryDate || counter <= 10) {
        // while (deliveryObj.index != deliveryDate || counter < 10) {
        while (deliveryObj.index != deliveryDate) {
            date = moment(date).add(1, 'day').format("YYYY-MM-DD");
            const dayOfWeek = moment(date).day();
            deliveryDate = dayOfWeek;
        }
        // const deliveryDate = moment(emailDate).day(deliveryObj.index).format("YYYY/MM/DD");
        return date
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

    if (daysToNextDelivery > 1) {
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

    const nextDeliveryDay = deliveryDayIndexes[nextIndex % deliveryDayIndexes.length];
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