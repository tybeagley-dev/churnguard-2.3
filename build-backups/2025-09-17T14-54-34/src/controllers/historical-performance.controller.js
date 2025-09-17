import { getHistoricalPerformanceData } from '../services/historical-performance.service.js';

export const getHistoricalPerformance = async (req, res) => {
  try {
    const data = await getHistoricalPerformanceData();
    res.json(data);
  } catch (error) {
    console.error('Error fetching historical performance:', error);
    res.status(500).json({ error: 'Failed to fetch historical performance data' });
  }
};