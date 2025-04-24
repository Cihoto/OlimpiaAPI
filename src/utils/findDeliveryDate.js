// const moment = require('moment'); // Import moment.js for date manipulation
// require('moment-timezone'); // Import moment-timezone for timezone support
// moment.tz.setDefault('America/Santiago'); // Set default timezone to Chile's timezone
// import moment from 'moment'; // Import moment.js for date manipulation



import moment from 'moment-timezone'; // Import moment-timezone for timezone support
moment.tz.setDefault('America/Santiago'); // Set default timezone to Chile's timezone

function findDeliveryDayByComuna(comunaToSearch){

    console.log("comunaToSearch", comunaToSearch.toLowerCase());

    if(!comunaToSearch || typeof comunaToSearch !== 'string') {
        return null; // Invalid input
    }
    console.log("comunaToSearch 2", comunaToSearch);
    const deliveryDays = [
        { "comuna": "SANTIAGO CENTRO", "dia": "LUNES" },
        { "comuna": "LAS CONDES", "dia": "LUNES" },
        { "comuna": "PROVIDENCIA", "dia": "LUNES" },
        { "comuna": "ÑUÑOA", "dia": "LUNES" },
        { "comuna": "VITACURA", "dia": "LUNES" },
        { "comuna": "LO BARNECHEA", "dia": "LUNES" },
        { "comuna": "ESTACIÓN CENTRAL", "dia": "LUNES" },
        { "comuna": "RECOLETA", "dia": "LUNES" },
        { "comuna": "COLINA", "dia": "LUNES" },
        { "comuna": "HUECHURABA", "dia": "LUNES" },
        { "comuna": "INDEPENDENCIA", "dia": "LUNES" },
        { "comuna": "QUILICURA", "dia": "LUNES" },
        { "comuna": "LO ESPEJO", "dia": "MARTES" },
        { "comuna": "MAIPÚ", "dia": "MARTES" },
        { "comuna": "SAN BERNARDO", "dia": "MARTES" },
        { "comuna": "LA FLORIDA", "dia": "MARTES" },
        { "comuna": "PEÑALOLÉN", "dia": "MARTES" },
        { "comuna": "SAN MIGUEL", "dia": "MARTES" },
        { "comuna": "EL BOSQUE", "dia": "MARTES" },
        { "comuna": "LA REINA", "dia": "MARTES" },
        { "comuna": "PROVIDENCIA", "dia": "MARTES" },
        { "comuna": "LAS CONDES", "dia": "MARTES" },
        { "comuna": "VITACURA", "dia": "MARTES" },
        { "comuna": "LA CISTERNA", "dia": "MARTES" },
        { "comuna": "CERRILLOS", "dia": "MARTES" },
        { "comuna": "MACUL", "dia": "MARTES" },
        { "comuna": "ÑUÑOA", "dia": "MARTES" },
        { "comuna": "SANTIAGO CENTRO", "dia": "MIÉRCOLES" },
        { "comuna": "LAS CONDES", "dia": "MIÉRCOLES" },
        { "comuna": "PROVIDENCIA", "dia": "MIÉRCOLES" },
        { "comuna": "ÑUÑOA", "dia": "MIÉRCOLES" },
        { "comuna": "VITACURA", "dia": "MIÉRCOLES" },
        { "comuna": "LO BARNECHEA", "dia": "MIÉRCOLES" },
        { "comuna": "ESTACIÓN CENTRAL", "dia": "MIÉRCOLES" },
        { "comuna": "RECOLETA", "dia": "MIÉRCOLES" },
        { "comuna": "COLINA", "dia": "MIÉRCOLES" },
        { "comuna": "HUECHURABA", "dia": "MIÉRCOLES" },
        { "comuna": "INDEPENDENCIA", "dia": "MIÉRCOLES" },
        { "comuna": "LO ESPEJO", "dia": "JUEVES" },
        { "comuna": "MAIPÚ", "dia": "JUEVES" },
        { "comuna": "SAN BERNARDO", "dia": "JUEVES" },
        { "comuna": "LA FLORIDA", "dia": "JUEVES" },
        { "comuna": "PEÑALOLÉN", "dia": "JUEVES" },
        { "comuna": "SAN MIGUEL", "dia": "JUEVES" },
        { "comuna": "EL BOSQUE", "dia": "JUEVES" },
        { "comuna": "LA REINA", "dia": "JUEVES" },
        { "comuna": "PROVIDENCIA", "dia": "JUEVES" },
        { "comuna": "LAS CONDES", "dia": "JUEVES" },
        { "comuna": "VITACURA", "dia": "JUEVES" },
        { "comuna": "LA CISTERNA", "dia": "JUEVES" },
        { "comuna": "CERRILLOS", "dia": "JUEVES" },
        { "comuna": "MACUL", "dia": "JUEVES" },
        { "comuna": "ÑUÑOA", "dia": "JUEVES" },
        { "comuna": "SANTIAGO CENTRO", "dia": "VIERNES" },
        { "comuna": "LAS CONDES", "dia": "VIERNES" },
        { "comuna": "PROVIDENCIA", "dia": "VIERNES" },
        { "comuna": "ÑUÑOA", "dia": "VIERNES" },
        { "comuna": "VITACURA", "dia": "VIERNES" },
        { "comuna": "LO BARNECHEA", "dia": "VIERNES" },
        { "comuna": "ESTACIÓN CENTRAL", "dia": "VIERNES" },
        { "comuna": "RECOLETA", "dia": "VIERNES" },
        { "comuna": "COLINA", "dia": "VIERNES" },
        { "comuna": "HUECHURABA", "dia": "VIERNES" },
        { "comuna": "INDEPENDENCIA", "dia": "VIERNES" },
        { "comuna": "QUILICURA", "dia": "VIERNES" }
    ];


    const matchingDeliveries = deliveryDays.filter((delivery) => {
        const normalize = (str) => str.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().trim();
        return normalize(delivery.comuna) === normalize(comunaToSearch);
    });

    console.log("matchingDeliveries", matchingDeliveries);

    if (matchingDeliveries.length > 0) {
        const now = moment();
        console.log("now", now.format('YYYY-MM-DD HH:mm:ss'));
        const cutoffHour = 8; // 12 AM cutoff time
        const daysOfWeek = ["DOMINGO", "LUNES", "MARTES", "MIÉRCOLES", "JUEVES", "VIERNES", "SÁBADO"];
        let closestDate = null;
        let minDaysUntilNextTarget = Infinity;

        matchingDeliveries.forEach((delivery) => {
            const targetDay = delivery.dia.toUpperCase();
            const targetDayIndex = daysOfWeek.indexOf(targetDay);

            if (targetDayIndex !== -1) {
                let daysUntilNextTarget = (targetDayIndex - now.isoWeekday() + 7) % 7;

                // If the target day is today and the current time is before the cutoff hour
                if (daysUntilNextTarget === 0 && now.hour() < cutoffHour) {
                    closestDate = now.format('YYYY-MM-DD');
                    minDaysUntilNextTarget = 0; // No need to check further
                } else if (daysUntilNextTarget > 0 || now.hour() >= cutoffHour) {
                    if (daysUntilNextTarget === 0) {
                        daysUntilNextTarget = 7; // Move to the next week's target day
                    }
                    if (daysUntilNextTarget < minDaysUntilNextTarget) {
                        minDaysUntilNextTarget = daysUntilNextTarget;
                        closestDate = now.clone().add(daysUntilNextTarget, 'days').format('YYYY-MM-DD');
                    }
                }
            }
        });

        return closestDate;
    } else {
        return null; // or handle the case when no match is found
    }
    
}

// module.exports = findDeliveryDayByComuna; // Replace export default
// with module.exports for CommonJS compatibility
// export default findDeliveryDayByComuna; // Uncomment this line if using ES6 modules

export default findDeliveryDayByComuna; // Export the function for use in other files