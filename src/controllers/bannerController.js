import OpenAI from "openai"

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

const BANNED_BUSINESS = [
    "del pedregal",
    "COMERCIAL QUINTO CENTRO",
    "COMERCIAL QUINTO CENTRO SPA.",
    "QUINTO CENTRO",
    "QUINTO CENTRO SPA",,
    "keylogistics",
    "keylogistics.cl",
    "keyLogistics (ESMAX)",
    ""

]

async function checkifBusinessIsBanned (req,res){
    // if(!req.apiKey) {
    //     res.status(401).json({code:401, error: 'Error al autenticar solicitud' });
    //     return;
    // }
    try {
        const plainText = req.body;

        const sanitizedEmailBody = plainText.replace(/[^a-zA-Z0-9\s]/g, '');  
        console.log("sanitizedEmailBody", sanitizedEmailBody);
      

        const systemPrompt = `Tu tarea es analizar un texto y buscar la empresa a la que pertenece el correo, posteriormente debes acceder a esta lista de empresas prohibidas: ${BANNED_BUSINESS} 
        y si el correo pertenece a una empresa de la lista debes devolver un json con el siguiente formato:
        Ten en cuenta que este correo recibe pedidos para nuestro cliente Olimpia o pedidos franui.
        {
            "business": "nombre de la empresa",
            "banned": true
        }
        debes buscar de forma flexible en el texto, no buscar de manera exacta las palabras,
        si el correo no pertenece a una empresa de la lista debes devolver un json con el siguiente formato: 
        {
            "business": "nombre de la empresa",
            "banned": false
        } no quiero explicaciones ni nada mas, solo el json`;

        const userPrompt = `este es el texto que debes analizar:
        ${sanitizedEmailBody}
        `;
        const response = await client.chat.completions.create({
            model: 'gpt-4o',
            messages: [
                { role: 'system', content: systemPrompt },
                { role: 'user', content: userPrompt },
            ]
        });


        const jsonResponse = response.choices[0].message.content.trim();
        console.log("jsonResponse", jsonResponse);
        const sanitizedOutput = jsonResponse.replace(/```json|```/g, '').replace(/\n/g, '').replace(/\\/g, '');
        const validJson = JSON.parse(sanitizedOutput);
        console.log("validJson", validJson);
        res.json(validJson);
        return
    } catch (error) {
        console.error('Error checking business:', error);
        if (error.code && error.message) {
            res.status(error.code).json({	
                ...error,
                success:false
            });
        } else {
            res.status(500).json({ errorCode: 5000, errorMessage: 'Internal server error' });
        }
    }
}

export {checkifBusinessIsBanned};