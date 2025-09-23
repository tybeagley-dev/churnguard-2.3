import { getAccountsData } from '../services/accounts.service.js';

export const getAccounts = async (req, res) => {
  try {
    const data = await getAccountsData();
    res.json(data);
  } catch (error) {
    console.error('Error fetching accounts data:', error);
    res.status(500).json({ error: 'Failed to fetch accounts data' });
  }
};