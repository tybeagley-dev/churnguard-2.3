import express from 'express';
import { getHistoricalPerformance } from '../controllers/historical-performance.controller.js';

const router = express.Router();

router.get('/historical-performance', getHistoricalPerformance);

export default router;