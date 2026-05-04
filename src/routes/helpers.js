import Router from 'express';
import {
    readCSV,
    readEmailBody,
    readEmailBodyFromGmail,
    readManualOcBatchDedup,
    readManualOcExtractDate,
    readManualOcPreview,
    readManualOcDispatchPreview,
    readManualOcSubmit,
    syncKnowledgebaseHandler,
    preflightSyncKnowledgebaseHandler
} from '../controllers/helpersController.js';
import { webhookCapture } from '../middleware/webhookCapture.js';

const router = Router();

// Define routes
router.get('/readCSV', readCSV);
router.post('/readEmailBody', readEmailBody);
router.post('/readEmailBodyFromGmail', readEmailBodyFromGmail );
router.post('/manual-oc/batch-dedup', readManualOcBatchDedup);
router.post('/manual-oc/extract-date', readManualOcExtractDate);
router.post('/manual-oc/preview', readManualOcPreview);
router.post('/manual-oc/dispatch-preview', readManualOcDispatchPreview);
router.post('/manual-oc/submit', readManualOcSubmit);
router.post('/sync-knowledgebase', webhookCapture, syncKnowledgebaseHandler);
router.post('/preflight-sync', preflightSyncKnowledgebaseHandler);

export default router;
