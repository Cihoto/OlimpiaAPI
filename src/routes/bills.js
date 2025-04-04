import Router from 'express';
import { createBill } from '../controllers/billsController.js';
const router = Router();

// Define routes
router.get('/issueNewBill', createBill);

export default router;