import Router from 'express';
import { createBill, getBillById, preflightBill } from '../controllers/billsController.js';
const router = Router();

// Define routes
router.post('/issueNewBill', createBill);
router.post('/preflight', preflightBill);

router.get('/getBill/:billId',getBillById);

export default router;