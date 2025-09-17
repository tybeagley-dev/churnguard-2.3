import express from 'express';
import { getMonthlyTrends } from '../controllers/monthly-trends.controller.js';

const router = express.Router();

router.get('/monthly-trends', getMonthlyTrends);

export default router;