import express from 'express';
import { getAccountHistoryController, getAccountHistoryMonthlyController } from '../controllers/account-history.controller.js';

const router = express.Router();

router.get('/account-history/:accountId', getAccountHistoryController);
router.get('/account-history-monthly/:accountId', getAccountHistoryMonthlyController);

export default router;