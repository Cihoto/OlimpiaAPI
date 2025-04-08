import Router from 'express';
import { readCSV } from '../controllers/helpersController.js';
const router = Router();

// Define routes
router.get('/readCSV', readCSV);

export default router;
