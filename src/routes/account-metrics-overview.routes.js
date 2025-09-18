import express from 'express';
import { getAccountMetricsOverview } from '../controllers/account-metrics-weekly.controller.js';
import { getAccountMetricsMonthly } from '../controllers/account-metrics-monthly.controller.js';

const router = express.Router();

// Account Metrics Overview - Weekly Mode
router.get('/account-metrics-overview', getAccountMetricsOverview);

// Account Metrics Overview - Monthly Mode
router.get('/account-metrics-monthly', getAccountMetricsMonthly);

export default router;