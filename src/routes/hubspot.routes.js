import express from 'express';
import {
  testHubSpotConnection,
  syncAllAccounts,
  getHubSpotSampleData,
  getHubSpotStatus
} from '../controllers/hubspot.controller.js';

const router = express.Router();

router.get('/status', getHubSpotStatus);
router.get('/test-connection', testHubSpotConnection);
router.get('/sample-data', getHubSpotSampleData);
router.post('/sync', syncAllAccounts);

export default router;