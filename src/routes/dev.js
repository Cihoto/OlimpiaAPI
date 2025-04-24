import { Router } from "express";
import path from "path";
import { fileURLToPath } from "url";

const router = Router();

// Define __dirname for ES6 modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

router.get("/jsonBills", (req, res) => {
  if (req.headers['x-custom-header'] !== '123qwe') {
    return res.status(403).json({ error: 'Access denied. Invalid header value.' });
  }
  const filePath = path.join(__dirname, '..', 'controllers', 'bills.json');
  res.sendFile(filePath);
});

export default router;