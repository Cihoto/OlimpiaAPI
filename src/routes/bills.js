import Router from 'express';
import { createBill, getBillById} from '../controllers/billsController.js';
const router = Router();

// Define routes
router.post('/issueNewBill', createBill);

router.get('/getBill/:billId',getBillById);

export default router;