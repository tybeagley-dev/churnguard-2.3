import express from 'express';
import { getAccountMetricsOverview } from '../controllers/account-metrics-overview.controller.js';

const router = express.Router();

// Account Metrics Overview - A Mode (Weekly) & B Mode (Monthly)
router.get('/account-metrics-overview', getAccountMetricsOverview);

export default router;