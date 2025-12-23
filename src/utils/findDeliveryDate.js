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
    "MACUL",
    "CONCHALÍ",
    "PUDAHUEL"
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
        // Convert emailDate to Chile timezone and get the day index
        emailDate = moment.tz(emailDate, 'America/Santiago');
        console.log(`Debe ser traido a la zona horaria de Chile ${emailDate}`);
        const emailDateDayIndex = emailDate.day();
       
        // Solo obtener la hora local de la fecha sin convertir a otra zona horaria
        // Obtener la hora en horario chileno (America/Santiago)
        // const emailDateHour = moment.tz(emailDate, 'YYYY-MM-DDTHH:mm:ss.SSSZ', 'America/Santiago').hour();
        const emailDateHour = emailDate.hour();
        const emailDateFormatted = moment(emailDate).format("YYYY-MM-DD");

        let daysForNextDelivery = null;

        //encontrar el proximo indice de entrega
        for (let i = 0; i < deliveryDayIndexes.length; i++) {
            // console.log("deliveryDayIndexes", deliveryDayIndexes[i]);
            const deliveryDayIndex = deliveryDayIndexes[i].index;

            if (emailDateDayIndex == 6 || emailDateDayIndex == 0) {

                const hasMondayDelivery = deliveryDayIndexes.some(day => day.index === 1);

                console.log("¿La comuna tiene despacho los lunes?", hasMondayDelivery);
                console.log("¿La comuna tiene despacho los lunes?", hasMondayDelivery);
                
                if (hasMondayDelivery) {
                    deliveryIndex = moveForward(deliveryDayIndexes.length, i, 1)
                }else{
                    deliveryIndex = moveForward(deliveryDayIndexes.length, i, 0)
                }

                break;
            }

            if (emailDateDayIndex == 5) {
                console.log("____________________________a____________________________________________________")

                deliveryIndex = DeliveryDaySelector(deliveryDayIndexes, i, emailDateFormatted, emailDateHour)
                // daysForNextDelivery = deliveryIndex;
                // break;
                const daysToNextDelivery = diffToNextDeliveryDay(deliveryDayIndexes, i, emailDateFormatted);

                // deliveryIndex = daysToNextDelivery;
                // break;
                // if (daysToNextDelivery > 1) {
                //     deliveryIndex = moveForward(deliveryDayIndexes.length, i, 0)
                //     break;
                // }
                if (emailDateHour >= 12) {
                    const hasMondayDelivery = deliveryDayIndexes.some(day => day.index === 1);

                    if(hasMondayDelivery){
                        deliveryIndex = moveForward(deliveryDayIndexes.length, i, 1)
                    }else{
                        deliveryIndex = moveForward(deliveryDayIndexes.length, i, 0)
                    }
                    
                } else {
                    deliveryIndex = moveForward(deliveryDayIndexes.length, i, 0)
                }
                break;
            }

            if (deliveryDayIndex > emailDateDayIndex) {

                
                deliveryIndex = DeliveryDaySelector(deliveryDayIndexes, i, emailDateFormatted, emailDateHour)
                daysForNextDelivery = deliveryIndex;

                const daysToNextDelivery = diffToNextDeliveryDay(deliveryDayIndexes, i, emailDateFormatted);
                console.log("______daysToNextDelivery______", daysToNextDelivery);

                if (daysToNextDelivery > 1) {
                    deliveryIndex = moveForward(deliveryDayIndexes.length, i, 0)
                    break;
                }
                
                console.log("ES MENOR A UNO");
                console.log("emailDateHour", emailDateHour);

                if (emailDateHour >= 12) {
                    console.log("PASADA LA HORA DE CORTE", emailDateHour)
                    deliveryIndex = moveForward(deliveryDayIndexes.length, i, 1)
                } else {
                    console.log("ANTES DE LA HORA DE CORTE", emailDateHour)
                    deliveryIndex = moveForward(deliveryDayIndexes.length, i, 0)
                }
                break;
            }
        }

        // Get the day name using the deliveryIndex
        const deliveryObj = deliveryDayIndexes[deliveryIndex]
        console.log("deliveryIndex", deliveryIndex)
        console.log("deliveryObj", deliveryObj)
        // teniendo en cuenta deliveryObject.dayName que puede ser LUNES, MARTES, MIERCOLES, JUEVES, VIERNES, buscar la fecha futura mas cercana que sea igual a la fecha de entrega teniendo como punto de partida la fecha del correo
        let deliveryDate = null;
        let date = emailDate;
        // while (deliveryObj.index != deliveryDate || counter <= 10) {
        // while (deliveryObj.index != deliveryDate || counter < 10) {
        console.log("este es el valor a evaluar", daysForNextDelivery)
        if(daysForNextDelivery == 0){
            date = moment(emailDate).add("1",'days').format("YYYY-MM-DD");
        }else{
            while (deliveryObj.index != deliveryDate) {
                date = moment(date).add(1, 'day').format("YYYY-MM-DD");
                const dayOfWeek = moment(date).day();
                deliveryDate = dayOfWeek;
            }
        }
        // const deliveryDate = moment(emailDate).day(deliveryObj.index).format("YYYY/MM/DD");
        console.log("date", date)
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