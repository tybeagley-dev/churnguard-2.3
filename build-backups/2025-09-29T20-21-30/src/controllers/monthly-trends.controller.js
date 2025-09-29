import { getMonthlyTrendsData } from '../services/monthly-trends.service.js';

export const getMonthlyTrends = async (req, res) => {
  try {
    const data = await getMonthlyTrendsData();
    res.json(data);
  } catch (error) {
    console.error('Error fetching monthly trends:', error);
    res.status(500).json({ error: 'Failed to fetch monthly trends data' });
  }
};