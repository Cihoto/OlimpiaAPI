import OpenAI  from 'openai';
const client = new OpenAI();

async function readMailBody(req, res) {
    const {emailBody,emailSubject, senderEmailAdress } = req.body; // Get the mail body from the request body

    if (!mailBody) {
        return res.status(400).json({ error: 'Mail body is required' });
    }

    try {
        const completion = await openai.chat.completions.create({
            model: "gpt-4o-mini",
            messages: [
              { "role": "system", "content": `You will be provided with a user query. Your goal is to extract a few keywords from the text to perform a search.\nKeep the search query to a few keywords that capture the user's intent.\nOnly output the keywords, without any additional text.` },
              { "role": "user", "content": `I'm having a hard time figuring out how to make sure my data disappears after 30 days of inactivity.\nCan you help me find out?` }
            ],
          });

          
        const response = await client.chat.completions.create({
            model: 'gpt-4o-mini',
            messages: [
                { role: 'user', content: `Extrae el RUT de la siguiente cadena de texto: ${mailBody}` },
            ],
            max_tokens: 100,
            temperature: 0.7,
        });

        const rut = response.choices[0].message.content.trim(); // Extract the RUT from the response

        res.status(200).json({ rut });
    } catch (error) {
        console.error('Error:', error);
        res.status(500).json({ error: 'Error processing the request' });
    }
}


export {readMailBody};