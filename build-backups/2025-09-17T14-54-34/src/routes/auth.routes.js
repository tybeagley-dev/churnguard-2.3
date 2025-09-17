import express from 'express';
import { login, logout, check, changePassword } from '../controllers/auth.controller.js';

const router = express.Router();

router.post('/auth/login', login);
router.get('/auth/check', check);
router.post('/auth/logout', logout);
router.post('/auth/change-password', changePassword);

export default router;