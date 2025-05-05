

function foundSpecialCustomers(customerName) {
    
    const customer = specialCustomers.find(({name}) => removeAccents(name).toLowerCase() === removeAccents(name).toLowerCase());
    if (customer) {
        return customer.rut;
    } else {
        return null;
    }
}

function removeAccents(str) {
    if (typeof str !== "string") {
        return "000000000000000000";
    }
    return str.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

const specialCustomers = [
    {
        names: [
            "RENDIC HERMANOS",
        ],
        rut: "",
    },
    {
        names: [
            "Quinto Centro Spa",
            "Comercial Quinto Centro Spa"
        ],
        rut: "",
    },
    {
        names: ["Comercial Las Invernadas S.A.", "Comercial Las Invernadas"],
        rut: "",
    },
    {
        names: [
            "ESMAX RED LTDA",
            "ESMAX RED LTDA (OWNER)"
        ],
        rut: "",
    },
    {
        names: [
            "JUST BURGER SPA",
            "Just Burger",
        ],
        rut: "",
    },
    {
        names: [
            "Get It",
            "Get It Los Militares 5890",
            "Get It San Pío",
            "Getit",
            "Getit Alameda",
            "Getit Chile"
        ],
        email: "getit.cl",
        rut: "",
    },
    {
        names: [
            "FREST",
            "FREST SPA"
        ],
        rut: "",
    },
    {
        names: [
            "Pedidos Ya - Franui Chile"
        ],
        rut: "",
    },
    {
        names: [
            "Convenience de Chile"  
        ],
        rut: "76.865.177-9",
    }
];

export default foundSpecialCustomers;