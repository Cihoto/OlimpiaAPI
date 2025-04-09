import Router from 'express';
import { createBill } from '../controllers/billsController.js';
const router = Router();

// Define routes
router.post('/issueNewBill', createBill);

export default router;