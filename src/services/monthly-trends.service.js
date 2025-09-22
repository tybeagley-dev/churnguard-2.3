import { getSharedDatabase } from '../../config/database.js';

export const getMonthlyTrendsData = async () => {
  const db = await getSharedDatabase();

  const query = `
    SELECT
      mm.month,
      COUNT(DISTINCT mm.account_id) as total_accounts,

      -- Use trending_risk_level for current month, historical_risk_level for completed months
      SUM(CASE WHEN COALESCE(mm.trending_risk_level, mm.historical_risk_level) = 'high' THEN 1 ELSE 0 END) as high_risk,
      SUM(CASE WHEN COALESCE(mm.trending_risk_level, mm.historical_risk_level) = 'medium' THEN 1 ELSE 0 END) as medium_risk,
      SUM(CASE WHEN COALESCE(mm.trending_risk_level, mm.historical_risk_level) = 'low' THEN 1 ELSE 0 END) as low_risk

    FROM monthly_metrics mm
    INNER JOIN accounts a ON mm.account_id = a.account_id
    WHERE (
      -- Account eligibility: launched by month-end, not archived before month-start
      a.launched_at IS NOT NULL
      AND a.launched_at < (mm.month || '-01')::date + INTERVAL '1 month'
      AND (
        COALESCE(a.archived_at, a.earliest_unit_archived_at) IS NULL
        OR COALESCE(a.archived_at, a.earliest_unit_archived_at)::date >= (mm.month || '-01')::date
      )
    )
    AND mm.month >= TO_CHAR(CURRENT_DATE - INTERVAL '12 months', 'YYYY-MM')
    AND mm.month <= TO_CHAR(CURRENT_DATE, 'YYYY-MM')
    GROUP BY mm.month
    ORDER BY mm.month ASC
  `;

  const result = await db.query(query);
  const results = result.rows;
  const currentMonth = new Date().toISOString().slice(0, 7); // YYYY-MM format

  // Transform data with month labels and current month indicator
  return results.map(row => ({
    month: row.month,
    month_label: formatMonthLabel(row.month),
    total_accounts: row.total_accounts || 0,
    high_risk: row.high_risk || 0,
    medium_risk: row.medium_risk || 0,
    low_risk: row.low_risk || 0,
    is_current_month: row.month === currentMonth
  }));
};

const formatMonthLabel = (monthStr) => {
  const [year, month] = monthStr.split('-');
  const monthIndex = parseInt(month, 10) - 1;
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  return `${months[monthIndex]} ${year}`;
};