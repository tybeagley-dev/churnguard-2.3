import express from 'express';
import { getAccounts } from '../controllers/accounts.controller.js';

const router = express.Router();

router.get('/accounts', getAccounts);

export default router;