import {
  getCurrentWeekData,
  getPreviousWtdData,
  getPrevious6WeekAvgData,
  getSameWtdLastMonthData,
  getSameWtdLastYearData
} from '../services/weekly-view.service.js';

export const getWeeklyView = async (req, res) => {
  try {
    const { period = 'current_week' } = req.query;

    let data;
    switch (period) {
      case 'current_week':
        data = await getCurrentWeekData();
        break;
      case 'vs_previous_wtd':
        data = await getPreviousWtdData();
        break;
      case 'vs_previous_6_week_avg':
        data = await getPrevious6WeekAvgData();
        break;
      case 'vs_same_wtd_last_month':
        data = await getSameWtdLastMonthData();
        break;
      case 'vs_same_wtd_last_year':
        data = await getSameWtdLastYearData();
        break;
      default:
        return res.status(400).json({
          error: `Unsupported period: ${period}. Supported periods: current_week, vs_previous_wtd, vs_previous_6_week_avg, vs_same_wtd_last_month, vs_same_wtd_last_year`
        });
    }

    res.json(data);
  } catch (error) {
    console.error('Error fetching weekly view data:', error);
    res.status(500).json({ error: 'Failed to fetch weekly view data' });
  }
};