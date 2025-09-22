import { getSharedDatabase } from '../../config/database.js';

export const getHistoricalPerformanceData = async () => {
  const db = await getSharedDatabase();

  const query = `
    SELECT
      mm.month,
      mm.month_label,
      COUNT(DISTINCT mm.account_id) as total_accounts,
      SUM(mm.total_spend) as total_spend,
      SUM(mm.total_coupons_redeemed) as total_redemptions,
      SUM(mm.avg_active_subs_cnt) as total_subscribers,
      SUM(mm.total_texts_delivered) as total_texts_sent
    FROM monthly_metrics mm
    INNER JOIN accounts a ON mm.account_id = a.account_id
    WHERE mm.month >= TO_CHAR(CURRENT_DATE - INTERVAL '12 months', 'YYYY-MM')
    AND mm.month < TO_CHAR(CURRENT_DATE, 'YYYY-MM')
    AND (
      -- Account eligibility: launched by month-end, not archived before month-start
      a.launched_at IS NOT NULL
      AND a.launched_at < (mm.month || '-01')::date + INTERVAL '1 month'
      AND (
        COALESCE(a.archived_at, a.earliest_unit_archived_at) IS NULL
        OR COALESCE(a.archived_at, a.earliest_unit_archived_at)::date >= (mm.month || '-01')::date
      )
    )
    GROUP BY mm.month, mm.month_label
    ORDER BY mm.month ASC
  `;

  const result = await db.query(query);
  const results = result.rows;

  // Transform month labels to abbreviated format (Jan 2025, Feb 2025, etc.)
  return results.map(row => ({
    month: row.month,
    month_label: formatMonthLabel(row.month),
    total_accounts: row.total_accounts || 0,
    total_spend: row.total_spend || 0,
    total_redemptions: row.total_redemptions || 0,
    total_subscribers: Math.round(row.total_subscribers || 0),
    total_texts_sent: row.total_texts_sent || 0
  }));
};

const formatMonthLabel = (monthStr) => {
  const [year, month] = monthStr.split('-');
  const monthIndex = parseInt(month, 10) - 1;
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  return `${months[monthIndex]} ${year}`;
};