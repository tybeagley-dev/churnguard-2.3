import { getSharedDatabase } from '../../config/database.js';
import { ChurnGuardCalendar } from '../utils/calendar.js';

export const getAccountHistoryMonthly = async (accountId) => {
  const db = await getSharedDatabase();

  console.log(`📈 Account History - Fetching monthly data for account: ${accountId}`);

  // Get the last 13 months of data for the account from monthly_metrics (12 complete + current)
  const monthlyData = await db.all(`
    SELECT
      month as month_yr,
      month_label,
      total_spend,
      total_texts_delivered,
      total_coupons_redeemed as coupons_redeemed,
      ROUND(avg_active_subs_cnt) as active_subs_cnt,
      historical_risk_level as risk_level,
      risk_reasons
    FROM monthly_metrics
    WHERE account_id = ?
    ORDER BY month DESC
    LIMIT 13
  `, accountId);

  return monthlyData || [];
};

export const getAccountHistory = async (accountId) => {
  const db = await getSharedDatabase();

  console.log(`📈 Account History - Fetching weekly data for account: ${accountId}`);

  // Get the last 12 weeks of data for the account
  const currentDate = new Date();
  const weekHistory = [];

  for (let weekOffset = 0; weekOffset < 12; weekOffset++) {
    const weekStart = new Date(currentDate);
    weekStart.setDate(currentDate.getDate() - (weekOffset * 7));

    // Get Sunday of that week
    const dayOfWeek = weekStart.getDay();
    const sundayOffset = dayOfWeek; // 0 = Sunday, 1 = Monday, etc.
    weekStart.setDate(weekStart.getDate() - sundayOffset);

    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekStart.getDate() + 6); // Saturday

    const weekStartISO = ChurnGuardCalendar.formatDateISO(weekStart);
    const weekEndISO = ChurnGuardCalendar.formatDateISO(weekEnd);

    // Get weekly aggregated data
    const weekData = await db.get(`
      SELECT
        SUM(total_spend) as total_spend,
        SUM(total_texts_delivered) as total_texts_delivered,
        SUM(coupons_redeemed) as coupons_redeemed,
        AVG(active_subs_cnt) as active_subs_cnt
      FROM daily_metrics
      WHERE account_id = ?
        AND date >= ?
        AND date <= ?
    `, accountId, weekStartISO, weekEndISO);

    // Get ISO week number for labeling
    const year = weekStart.getFullYear();
    const weekNumber = getWeekNumber(weekStart);

    // Format date string properly
    const dateStr = weekStart.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric'
    });

    // Format week label for breakdown list: "Week 26 | Jun 29, 2025"
    const weekLabel = `Week ${weekNumber} | ${dateStr}`;

    // Short week label for chart: "Week 26"
    const weekYr = `Week ${weekNumber}`;


    weekHistory.push({
      week_yr: weekYr,
      week_label: weekLabel,
      total_spend: weekData?.total_spend || 0,
      total_texts_delivered: weekData?.total_texts_delivered || 0,
      coupons_redeemed: weekData?.coupons_redeemed || 0,
      active_subs_cnt: Math.round(weekData?.active_subs_cnt || 0)
    });
  }

  // Return in reverse chronological order (most recent first)
  return weekHistory.reverse();
};

// Helper function to get ISO week number
function getWeekNumber(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}

