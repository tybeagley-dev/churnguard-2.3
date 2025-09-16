import express from 'express';
import { getWeeklyView } from '../controllers/weekly-view.controller.js';

const router = express.Router();

router.get('/weekly-view', getWeeklyView);

export default router;