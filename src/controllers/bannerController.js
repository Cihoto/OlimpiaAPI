import OpenAI from "openai"

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

const BANNED_BUSINESS = [
    "del pedregal"
]

async function checkifBusinessIsBanned (req,res){
    if(!req.apiKey) {
        res.status(401).json({code:401, error: 'Error al autenticar solicitud' });
        return;
    }
    try {
        const plainText = req.body;

        const sanitizedEmailBody = plainText
        .replaceAll(/\s+/g, ' ') // Remove all white spaces
        .trim(); // Trim leading and trailing spaces

        const {emailContent, senderEmails, emailSubject} = JSON.parse(sanitizedEmailBody);

        const requiredFields = ["emailContent", "senderEmails", "emailSubject"];

        const missingFields = requiredFields.filter(field => !(field in JSON.parse(sanitizedEmailBody)));

        if (missingFields.length > 0) {
            console.log("Invalid request, missing fields:", missingFields);
            return res.status(400).json({ error: 'Invalid request body' });
        }

        const systemPrompt = `busca dentro de esta lista ${BANNED_BUSINESS} si el correo pertenece a una empresa de la lista,
        si el correo pertenece a una empresa de la lista debes devolver un json con el siguiente formato: 
        {
            "business": "nombre de la empresa",
            "banned": true
        }
        debes buscar de forma flexible en el texto, no buscar de manera exacta las palabras,
        si el correo no pertenece a una empresa de la lista debes devolver un json con el siguiente formato: 
        {
            "business": "nombre de la empresa",
            "banned": false
        }
        `;

        const userPrompt = `este es el texto que debes analizar:
        este es el contenido del correo: ${emailContent}
        este es el asunto del correo: ${emailSubject}
        este es el remitente del correo: ${senderEmails}
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