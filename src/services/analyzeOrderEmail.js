import { OpenAI } from 'openai';

const SYSTEM_INSTRUCTIONS = ''; // Instrucciones del sistema (puedes personalizarlas después)
const OLIMPIA_ASSISTANT_ID = process.env.OLIMPIA_ORDER_FINDER_ASSISTENT_ID; // ID del asistente en OpenAI Platform
const OPENAI_OLIMPIA_API_KEY = process.env.OPENAI_OLIMPIA; // Clave API de OpenAI


const openai = new OpenAI({ apiKey: OPENAI_OLIMPIA_API_KEY });
async function analyzeOrderEmail(emailContent) {


    try {
        const thread = await openai.beta.threads.create();
        const threadId = thread.id;

        let activeRun;
        do {
            const runs = await openai.beta.threads.runs.list(threadId);
            activeRun = runs.data.find(run => run.status === 'active');

            if (activeRun) {
                console.log(`⏳ Esperando a que termine el run activo: ${activeRun.id}`);
                await new Promise(resolve => setTimeout(resolve, 1000)); // Espera 1 segundo
            }
        } while (activeRun);

        // Agrega mensaje del usuario al thread
        await openai.beta.threads.messages.create(threadId, {
            role: "user",
            content: emailContent
        });

        // Ejecuta el assistant
        const run = await openai.beta.threads.runs.create(threadId, {
            assistant_id: OLIMPIA_ASSISTANT_ID
        });

        // Espera que termine el procesamiento
        let runStatus;
        do {
            await new Promise(resolve => setTimeout(resolve, 1000));
            runStatus = await openai.beta.threads.runs.retrieve(threadId, run.id);
        } while (runStatus.status !== 'completed');

        // Recupera la última respuesta del bot
        const messages = await openai.beta.threads.messages.list(threadId);
        const assistantResponse = messages.data.find(m => m.role === 'assistant');
        console.log('assistantResponse', assistantResponse);
        const reply = parseToJson(assistantResponse?.content?.[0]?.text?.value || 'Sin respuesta');;

        return reply;
    } catch (error) {
        return {
            Pedido_Cantidad_Pink : 0,
            Pedido_Cantidad_Amargo: 0,
            Pedido_Cantidad_Leche: 0
        }
    }
}

function parseToJson(reply) {
    try {
        // Intenta parsear el reply a JSON
        const parsed = JSON.parse(reply);
        return parsed;
    } catch (error) {
        // Si no es posible parsear, devuelve el reply sin cambios
        console.warn("⚠️ No se pudo parsear el reply a JSON:", error.message);
        return reply;
    }
}

export { analyzeOrderEmail };