import Router from 'express';
import { readCSV, readEmailBody, readEmailBodyFromGmail } from '../controllers/helpersController.js';

const router = Router();

// Define routes
router.get('/readCSV', readCSV);
router.post('/readEmailBody', readEmailBody);
router.post('/readEmailBodyFromGmail', readEmailBodyFromGmail );

export default router;
