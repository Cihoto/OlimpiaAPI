import Router from 'express';
import { readCSV,readEmailBody} from '../controllers/helpersController.js';

const router = Router();

// Define routes
router.get('/readCSV', readCSV);
router.post('/readEmailBody', readEmailBody);

export default router;
