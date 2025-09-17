import { getSharedDatabase } from '../../config/database.js';
import { ChurnGuardCalendar } from '../utils/calendar.js';

export const getAccountsData = async () => {
  const db = await getSharedDatabase();
  const currentMonth = ChurnGuardCalendar.getCurrentMonth();

  console.log(`📊 Accounts - Fetching data for ${currentMonth}`);

  const accounts = await db.all(`
    SELECT
      a.account_id,
      a.account_name,
      a.status,
      a.csm_owner,
      a.launched_at,
      mm.trending_risk_level,
      mm.avg_active_subs_cnt as total_subscribers,
      mm.total_spend,
      mm.total_texts_delivered,
      mm.total_coupons_redeemed
    FROM accounts a
    INNER JOIN monthly_metrics mm ON a.account_id = mm.account_id
      AND mm.month = ?
      AND mm.trending_risk_level IS NOT NULL
    WHERE (
      -- Account eligibility: launched by month-end, not archived before month-start
      DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
      AND (
        (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
        OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(? || '-01')
      )
    )
    ORDER BY a.account_name ASC
  `, currentMonth, currentMonth, currentMonth);

  return {
    accounts,
    total_count: accounts.length,
    month: currentMonth
  };
};