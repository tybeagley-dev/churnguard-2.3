import { getAccountHistory, getAccountHistoryMonthly } from '../services/account-history.service.js';

export const getAccountHistoryMonthlyController = async (req, res) => {
  try {
    const { accountId } = req.params;

    if (!accountId) {
      return res.status(400).json({
        error: 'Account ID is required'
      });
    }

    const monthlyData = await getAccountHistoryMonthly(accountId);

    res.set({
      'Cache-Control': 'no-cache, no-store, must-revalidate',
      'Pragma': 'no-cache',
      'Expires': '0'
    });

    res.json(monthlyData);
  } catch (error) {
    console.error('Monthly Account History Error:', error);
    res.status(500).json({
      error: 'Failed to fetch monthly account history'
    });
  }
};

export const getAccountHistoryController = async (req, res) => {
  try {
    const { accountId } = req.params;

    if (!accountId) {
      return res.status(400).json({
        error: 'Account ID is required'
      });
    }

    const weeklyData = await getAccountHistory(accountId);

    res.set({
      'Cache-Control': 'no-cache, no-store, must-revalidate',
      'Pragma': 'no-cache',
      'Expires': '0'
    });

    res.json(weeklyData);
  } catch (error) {
    console.error('Account History Error:', error);
    res.status(500).json({
      error: 'Failed to fetch account history'
    });
  }
};