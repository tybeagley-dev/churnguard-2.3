import { getSharedDatabase } from '../../config/database.js';
import { ChurnGuardCalendar } from '../utils/calendar.js';

const getAccountMetricsDataForPeriod = async (weekStart, weekEnd, month, label = '') => {
  const db = await getSharedDatabase();

  console.log(`📊 Account Metrics Overview - ${label}: ${weekStart} to ${weekEnd}`);

  const accounts = await db.all(`
    SELECT
      a.account_id,
      a.account_name,
      a.status,
      a.csm_owner,
      a.launched_at,

      -- Period totals from daily_metrics (default to 0 if no activity)
      COALESCE(period_data.total_spend, 0) as total_spend,
      COALESCE(period_data.total_texts_delivered, 0) as total_texts_delivered,
      COALESCE(period_data.total_coupons_redeemed, 0) as total_coupons_redeemed,
      COALESCE(ROUND(COALESCE(period_data.avg_active_subs_cnt, mm.avg_active_subs_cnt)), 0) as total_subscribers

    FROM accounts a
    INNER JOIN monthly_metrics mm ON a.account_id = mm.account_id
      AND mm.month = ?
      AND mm.trending_risk_level IS NOT NULL
    LEFT JOIN (
      SELECT
        account_id,
        AVG(active_subs_cnt) as avg_active_subs_cnt,
        SUM(coupons_redeemed) as total_coupons_redeemed,
        SUM(total_spend) as total_spend,
        SUM(total_texts_delivered) as total_texts_delivered
      FROM daily_metrics
      WHERE date >= ? AND date <= ?
      GROUP BY account_id
    ) period_data ON a.account_id = period_data.account_id
    ORDER BY a.account_name ASC
  `, month, weekStart, weekEnd);

  // Calculate aggregated totals for upper portion (summary cards)
  const totals = accounts.reduce((acc, account) => {
    acc.total_spend += account.total_spend;
    acc.total_texts_delivered += account.total_texts_delivered;
    acc.total_coupons_redeemed += account.total_coupons_redeemed;
    acc.total_subscribers += account.total_subscribers;
    return acc;
  }, {
    total_spend: 0,
    total_texts_delivered: 0,
    total_coupons_redeemed: 0,
    total_subscribers: 0
  });

  return {
    date_range: { start: weekStart, end: weekEnd },
    metrics: {
      total_spend: totals.total_spend,
      total_texts: totals.total_texts_delivered,
      total_redemptions: totals.total_coupons_redeemed,
      total_subscribers: totals.total_subscribers
    },
    accounts: accounts.map(account => ({
      account_id: account.account_id,
      name: account.account_name,
      csm: account.csm_owner,
      status: account.status,
      launched_at: account.launched_at,
      total_spend: account.total_spend,
      total_texts_delivered: account.total_texts_delivered,
      coupons_redeemed: account.total_coupons_redeemed,
      active_subs_cnt: account.total_subscribers
    }))
  };
};

export const getCurrentWeekBaselineData = async () => {
  const calendarInfo = ChurnGuardCalendar.getDateInfo();
  const weekStart = calendarInfo.week.start;
  const weekEnd = calendarInfo.week.end;
  const currentMonth = ChurnGuardCalendar.getCurrentMonth();

  return await getAccountMetricsDataForPeriod(
    weekStart,
    weekEnd,
    currentMonth,
    'Current WTD Baseline'
  );
};

export const getComparisonData = async (comparisonPeriod) => {
  const calendarInfo = ChurnGuardCalendar.getDateInfo();
  const currentWeekStart = new Date(calendarInfo.week.start);
  const currentWeekEnd = new Date(calendarInfo.week.end);
  const currentMonth = ChurnGuardCalendar.getCurrentMonth();

  // Calculate how many days are in current WTD (to match comparison periods)
  const currentDayCount = Math.floor((currentWeekEnd - currentWeekStart) / (1000 * 60 * 60 * 24)) + 1;

  switch (comparisonPeriod) {
    case 'vs_previous_wtd': {
      // Previous week's Sunday, same number of days
      const prevWeekStart = new Date(currentWeekStart);
      prevWeekStart.setDate(currentWeekStart.getDate() - 7);
      const prevWeekEnd = new Date(prevWeekStart);
      prevWeekEnd.setDate(prevWeekStart.getDate() + currentDayCount - 1);

      return await getAccountMetricsDataForPeriod(
        ChurnGuardCalendar.formatDateISO(prevWeekStart),
        ChurnGuardCalendar.formatDateISO(prevWeekEnd),
        currentMonth,
        'Previous WTD'
      );
    }

    case 'vs_6_week_avg': {
      // Get data for each of the previous 6 weeks and calculate averages
      const weeklyData = [];

      for (let weekOffset = 1; weekOffset <= 6; weekOffset++) {
        const weekStart = new Date(currentWeekStart);
        weekStart.setDate(currentWeekStart.getDate() - (weekOffset * 7));
        const weekEnd = new Date(weekStart);
        weekEnd.setDate(weekStart.getDate() + currentDayCount - 1);

        const data = await getAccountMetricsDataForPeriod(
          ChurnGuardCalendar.formatDateISO(weekStart),
          ChurnGuardCalendar.formatDateISO(weekEnd),
          currentMonth,
          `Week ${weekOffset} ago`
        );
        weeklyData.push(data);
      }

      // Calculate averages across the 6 weeks
      const accountAverageMap = new Map();

      weeklyData.forEach(weekData => {
        weekData.accounts.forEach(account => {
          if (!accountAverageMap.has(account.account_id)) {
            accountAverageMap.set(account.account_id, {
              account_id: account.account_id,
              name: account.name,
              status: account.status,
              csm: account.csm,
              launched_at: account.launched_at,
              total_spend: 0,
              total_texts_delivered: 0,
              coupons_redeemed: 0,
              active_subs_cnt: 0
            });
          }

          const avgAccount = accountAverageMap.get(account.account_id);
          avgAccount.total_spend += account.total_spend;
          avgAccount.total_texts_delivered += account.total_texts_delivered;
          avgAccount.coupons_redeemed += account.coupons_redeemed;
          avgAccount.active_subs_cnt += account.active_subs_cnt;
        });
      });

      // Finalize averages (divide by 6)
      const averagedAccounts = Array.from(accountAverageMap.values()).map(account => ({
        ...account,
        total_spend: Math.round(account.total_spend / 6),
        total_texts_delivered: Math.round(account.total_texts_delivered / 6),
        coupons_redeemed: Math.round(account.coupons_redeemed / 6),
        active_subs_cnt: Math.round(account.active_subs_cnt / 6)
      }));

      // Calculate averaged metrics
      const avgMetrics = averagedAccounts.reduce((acc, account) => {
        acc.total_spend += account.total_spend;
        acc.total_texts += account.total_texts_delivered;
        acc.total_redemptions += account.coupons_redeemed;
        acc.total_subscribers += account.active_subs_cnt;
        return acc;
      }, { total_spend: 0, total_texts: 0, total_redemptions: 0, total_subscribers: 0 });

      const firstWeekStart = new Date(currentWeekStart);
      firstWeekStart.setDate(currentWeekStart.getDate() - (6 * 7));
      const lastWeekStart = new Date(currentWeekStart);
      lastWeekStart.setDate(currentWeekStart.getDate() - 7);
      const lastWeekEnd = new Date(lastWeekStart);
      lastWeekEnd.setDate(lastWeekStart.getDate() + currentDayCount - 1);

      return {
        date_range: {
          start: ChurnGuardCalendar.formatDateISO(firstWeekStart),
          end: ChurnGuardCalendar.formatDateISO(lastWeekEnd)
        },
        metrics: avgMetrics,
        accounts: averagedAccounts
      };
    }

    case 'vs_same_wtd_last_month': {
      // Go back 5 weeks (approximately last month)
      const lastMonthWeekStart = new Date(currentWeekStart);
      lastMonthWeekStart.setDate(currentWeekStart.getDate() - (5 * 7));
      const lastMonthWeekEnd = new Date(lastMonthWeekStart);
      lastMonthWeekEnd.setDate(lastMonthWeekStart.getDate() + currentDayCount - 1);

      return await getAccountMetricsDataForPeriod(
        ChurnGuardCalendar.formatDateISO(lastMonthWeekStart),
        ChurnGuardCalendar.formatDateISO(lastMonthWeekEnd),
        currentMonth,
        'Same WTD Last Month'
      );
    }

    case 'vs_same_wtd_last_year': {
      // Go back 52 weeks
      const lastYearWeekStart = new Date(currentWeekStart);
      lastYearWeekStart.setDate(currentWeekStart.getDate() - (52 * 7));
      const lastYearWeekEnd = new Date(lastYearWeekStart);
      lastYearWeekEnd.setDate(lastYearWeekStart.getDate() + currentDayCount - 1);

      return await getAccountMetricsDataForPeriod(
        ChurnGuardCalendar.formatDateISO(lastYearWeekStart),
        ChurnGuardCalendar.formatDateISO(lastYearWeekEnd),
        currentMonth,
        'Same WTD Last Year'
      );
    }

    default:
      throw new Error(`Unsupported comparison period: ${comparisonPeriod}`);
  }
};

export const calculateAccountDeltas = (baselineAccounts, comparisonAccounts) => {
  // Create lookup map for comparison accounts
  const comparisonMap = new Map();
  comparisonAccounts.forEach(account => {
    comparisonMap.set(account.account_id, account);
  });

  // Add deltas to baseline accounts
  return baselineAccounts.map(baselineAccount => {
    const comparisonAccount = comparisonMap.get(baselineAccount.account_id) || {
      total_spend: 0,
      total_texts_delivered: 0,
      coupons_redeemed: 0,
      active_subs_cnt: 0
    };

    return {
      ...baselineAccount,
      spend_delta: baselineAccount.total_spend - comparisonAccount.total_spend,
      texts_delta: baselineAccount.total_texts_delivered - comparisonAccount.total_texts_delivered,
      coupons_delta: baselineAccount.coupons_redeemed - comparisonAccount.coupons_redeemed,
      subs_delta: baselineAccount.active_subs_cnt - comparisonAccount.active_subs_cnt
    };
  });
};