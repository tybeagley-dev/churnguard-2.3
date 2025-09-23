import express from 'express';
import {
  getCurrentMonthBaseline,
  getMonthlyComparison
} from '../controllers/account-metrics-overview-monthly.controller.js';

const router = express.Router();

// Monthly View baseline data (current MTD from monthly_metrics)
router.get('/account-metrics-overview/monthly/baseline', getCurrentMonthBaseline);

// Monthly View comparison data with deltas
router.get('/account-metrics-overview/monthly/comparison/:period', getMonthlyComparison);

export default router;