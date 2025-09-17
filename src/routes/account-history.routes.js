import express from 'express';
import { getAccountHistoryController } from '../controllers/account-history.controller.js';

const router = express.Router();

router.get('/account-history/:accountId', getAccountHistoryController);

export default router;