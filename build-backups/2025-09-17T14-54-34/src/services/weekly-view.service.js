import { getSharedDatabase } from '../../config/database.js';
import { ChurnGuardCalendar } from '../utils/calendar.js';

const getWeeklyData = async (weekStart, weekEnd, month, label = '') => {
  const db = await getSharedDatabase();

  console.log(`📅 Weekly View - ${label}: ${weekStart} to ${weekEnd}`);

  const accounts = await db.all(`
    SELECT
      a.account_id,
      a.account_name,
      a.status,
      a.csm_owner,
      a.launched_at,

      -- Weekly totals from daily_metrics (default to 0 if no weekly activity)
      COALESCE(wtd.total_spend, 0) as total_spend,
      COALESCE(wtd.total_texts_delivered, 0) as total_texts_delivered,
      COALESCE(wtd.total_coupons_redeemed, 0) as total_coupons_redeemed,
      COALESCE(ROUND(COALESCE(wtd.avg_active_subs_cnt, mm.avg_active_subs_cnt)), 0) as total_subscribers

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
    ) wtd ON a.account_id = wtd.account_id
    WHERE (
      -- Account eligibility: launched by month-end, not archived before month-start
      DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
      AND (
        (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
        OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(? || '-01')
      )
    )
    ORDER BY a.account_name ASC
  `, month, weekStart, weekEnd, month, month);

  // Calculate aggregated totals from individual accounts
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
      account_name: account.account_name,
      status: account.status,
      csm_owner: account.csm_owner,
      launched_at: account.launched_at,
      total_spend: account.total_spend,
      total_texts: account.total_texts_delivered,
      total_redemptions: account.total_coupons_redeemed,
      total_subscribers: account.total_subscribers
    }))
  };
};

export const getCurrentWeekData = async () => {
  const calendarInfo = ChurnGuardCalendar.getDateInfo();
  const weekStart = calendarInfo.week.start;
  const weekEnd = calendarInfo.week.end;
  const currentMonth = ChurnGuardCalendar.getCurrentMonth();

  const data = await getWeeklyData(weekStart, weekEnd, currentMonth, 'Current WTD');

  return {
    period: 'current_week',
    ...data
  };
};

export const getPreviousWtdData = async () => {
  const calendarInfo = ChurnGuardCalendar.getDateInfo();
  const currentWeekStart = new Date(calendarInfo.week.start);
  const currentWeekEnd = new Date(calendarInfo.week.end);

  // Calculate how many days are in current WTD
  const currentDayCount = Math.floor((currentWeekEnd - currentWeekStart) / (1000 * 60 * 60 * 24)) + 1;

  // Get current WTD data
  const currentData = await getCurrentWeekData();

  // Previous week's Sunday
  const prevWeekStart = new Date(currentWeekStart);
  prevWeekStart.setDate(currentWeekStart.getDate() - 7);

  // Same number of days in previous week
  const prevWeekEnd = new Date(prevWeekStart);
  prevWeekEnd.setDate(prevWeekStart.getDate() + currentDayCount - 1);

  const weekStart = ChurnGuardCalendar.formatDateISO(prevWeekStart);
  const weekEnd = ChurnGuardCalendar.formatDateISO(prevWeekEnd);
  const currentMonth = ChurnGuardCalendar.getCurrentMonth();

  const comparisonData = await getWeeklyData(weekStart, weekEnd, currentMonth, 'Previous WTD');

  // Create lookup map for comparison account data
  const comparisonAccountMap = new Map();
  comparisonData.accounts.forEach(account => {
    comparisonAccountMap.set(account.account_id, account);
  });

  // Add deltas to current accounts
  const accountsWithDeltas = currentData.accounts.map(currentAccount => {
    const comparisonAccount = comparisonAccountMap.get(currentAccount.account_id) || {
      total_spend: 0,
      total_texts: 0,
      total_redemptions: 0,
      total_subscribers: 0
    };

    return {
      ...currentAccount,
      deltas: {
        total_spend: currentAccount.total_spend - comparisonAccount.total_spend,
        total_texts: currentAccount.total_texts - comparisonAccount.total_texts,
        total_redemptions: currentAccount.total_redemptions - comparisonAccount.total_redemptions,
        total_subscribers: currentAccount.total_subscribers - comparisonAccount.total_subscribers
      }
    };
  });

  return {
    period: 'vs_previous_wtd',
    date_range: currentData.date_range,
    metrics: currentData.metrics, // Current WTD metrics
    comparison_date_range: comparisonData.date_range,
    comparison_metrics: comparisonData.metrics, // Previous WTD metrics
    accounts: accountsWithDeltas
  };
};

export const getPrevious6WeekAvgData = async () => {
  const calendarInfo = ChurnGuardCalendar.getDateInfo();
  const currentWeekStart = new Date(calendarInfo.week.start);
  const currentWeekEnd = new Date(calendarInfo.week.end);

  // Calculate how many days are in current WTD
  const currentDayCount = Math.floor((currentWeekEnd - currentWeekStart) / (1000 * 60 * 60 * 24)) + 1;

  // Get current WTD data
  const currentData = await getCurrentWeekData();

  const weeklyAccountData = [];
  const currentMonth = ChurnGuardCalendar.getCurrentMonth();

  // Get data for each of the previous 6 weeks
  for (let weekOffset = 1; weekOffset <= 6; weekOffset++) {
    const weekStart = new Date(currentWeekStart);
    weekStart.setDate(currentWeekStart.getDate() - (weekOffset * 7));

    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekStart.getDate() + currentDayCount - 1);

    const weekStartISO = ChurnGuardCalendar.formatDateISO(weekStart);
    const weekEndISO = ChurnGuardCalendar.formatDateISO(weekEnd);

    const data = await getWeeklyData(weekStartISO, weekEndISO, currentMonth, `Week ${weekOffset} ago`);
    weeklyAccountData.push(data.accounts);
  }

  // Calculate average metrics for each account across the 6 weeks
  const accountAverageMap = new Map();

  weeklyAccountData.forEach(weekAccounts => {
    weekAccounts.forEach(account => {
      if (!accountAverageMap.has(account.account_id)) {
        accountAverageMap.set(account.account_id, {
          total_spend: 0,
          total_texts: 0,
          total_redemptions: 0,
          total_subscribers: 0,
          weekCount: 0
        });
      }

      const avgData = accountAverageMap.get(account.account_id);
      avgData.total_spend += account.total_spend;
      avgData.total_texts += account.total_texts;
      avgData.total_redemptions += account.total_redemptions;
      avgData.total_subscribers += account.total_subscribers;
      avgData.weekCount += 1;
    });
  });

  // Finalize averages
  accountAverageMap.forEach((avgData, accountId) => {
    avgData.total_spend = Math.round(avgData.total_spend / 6); // Divide by 6, not weekCount (some accounts may not appear in all weeks)
    avgData.total_texts = Math.round(avgData.total_texts / 6);
    avgData.total_redemptions = Math.round(avgData.total_redemptions / 6);
    avgData.total_subscribers = Math.round(avgData.total_subscribers / 6);
  });

  // Add deltas to current accounts
  const accountsWithDeltas = currentData.accounts.map(currentAccount => {
    const avgAccount = accountAverageMap.get(currentAccount.account_id) || {
      total_spend: 0,
      total_texts: 0,
      total_redemptions: 0,
      total_subscribers: 0
    };

    return {
      ...currentAccount,
      deltas: {
        total_spend: currentAccount.total_spend - avgAccount.total_spend,
        total_texts: currentAccount.total_texts - avgAccount.total_texts,
        total_redemptions: currentAccount.total_redemptions - avgAccount.total_redemptions,
        total_subscribers: currentAccount.total_subscribers - avgAccount.total_subscribers
      }
    };
  });

  const firstWeekStart = new Date(currentWeekStart);
  firstWeekStart.setDate(currentWeekStart.getDate() - (6 * 7));
  const lastWeekStart = new Date(currentWeekStart);
  lastWeekStart.setDate(currentWeekStart.getDate() - 7);
  const lastWeekEnd = new Date(lastWeekStart);
  lastWeekEnd.setDate(lastWeekStart.getDate() + currentDayCount - 1);

  // Calculate the averaged comparison metrics
  const weeklyMetricsData = [];
  for (let weekOffset = 1; weekOffset <= 6; weekOffset++) {
    const weekStart = new Date(currentWeekStart);
    weekStart.setDate(currentWeekStart.getDate() - (weekOffset * 7));
    const weekEnd = new Date(weekStart);
    weekEnd.setDate(weekStart.getDate() + currentDayCount - 1);
    const weekStartISO = ChurnGuardCalendar.formatDateISO(weekStart);
    const weekEndISO = ChurnGuardCalendar.formatDateISO(weekEnd);
    const data = await getWeeklyData(weekStartISO, weekEndISO, currentMonth, `Week ${weekOffset} metrics`);
    weeklyMetricsData.push(data.metrics);
  }

  const avgComparisonMetrics = weeklyMetricsData.reduce((acc, week, index) => {
    if (index === 0) {
      return { ...week };
    }
    acc.total_spend += week.total_spend;
    acc.total_texts += week.total_texts;
    acc.total_redemptions += week.total_redemptions;
    acc.total_subscribers += week.total_subscribers;
    return acc;
  }, {});

  // Divide by 6 for averages
  avgComparisonMetrics.total_spend = Math.round(avgComparisonMetrics.total_spend / 6);
  avgComparisonMetrics.total_texts = Math.round(avgComparisonMetrics.total_texts / 6);
  avgComparisonMetrics.total_redemptions = Math.round(avgComparisonMetrics.total_redemptions / 6);
  avgComparisonMetrics.total_subscribers = Math.round(avgComparisonMetrics.total_subscribers / 6);

  return {
    period: 'vs_previous_6_week_avg',
    date_range: currentData.date_range,
    metrics: currentData.metrics, // Current WTD metrics
    comparison_date_range: {
      start: ChurnGuardCalendar.formatDateISO(firstWeekStart),
      end: ChurnGuardCalendar.formatDateISO(lastWeekEnd)
    },
    comparison_metrics: avgComparisonMetrics, // 6-week average metrics
    accounts: accountsWithDeltas
  };
};

export const getSameWtdLastMonthData = async () => {
  const calendarInfo = ChurnGuardCalendar.getDateInfo();
  const currentWeekStart = new Date(calendarInfo.week.start);
  const currentWeekEnd = new Date(calendarInfo.week.end);

  // Calculate how many days are in current WTD
  const currentDayCount = Math.floor((currentWeekEnd - currentWeekStart) / (1000 * 60 * 60 * 24)) + 1;

  // Get current WTD data
  const currentData = await getCurrentWeekData();

  // Go back 5 weeks as suggested
  const lastMonthWeekStart = new Date(currentWeekStart);
  lastMonthWeekStart.setDate(currentWeekStart.getDate() - (5 * 7));

  // Same number of days
  const lastMonthWeekEnd = new Date(lastMonthWeekStart);
  lastMonthWeekEnd.setDate(lastMonthWeekStart.getDate() + currentDayCount - 1);

  const weekStart = ChurnGuardCalendar.formatDateISO(lastMonthWeekStart);
  const weekEnd = ChurnGuardCalendar.formatDateISO(lastMonthWeekEnd);
  const currentMonth = ChurnGuardCalendar.getCurrentMonth();

  const comparisonData = await getWeeklyData(weekStart, weekEnd, currentMonth, 'Same WTD Last Month');

  // Create lookup map for comparison account data
  const comparisonAccountMap = new Map();
  comparisonData.accounts.forEach(account => {
    comparisonAccountMap.set(account.account_id, account);
  });

  // Add deltas to current accounts
  const accountsWithDeltas = currentData.accounts.map(currentAccount => {
    const comparisonAccount = comparisonAccountMap.get(currentAccount.account_id) || {
      total_spend: 0,
      total_texts: 0,
      total_redemptions: 0,
      total_subscribers: 0
    };

    return {
      ...currentAccount,
      deltas: {
        total_spend: currentAccount.total_spend - comparisonAccount.total_spend,
        total_texts: currentAccount.total_texts - comparisonAccount.total_texts,
        total_redemptions: currentAccount.total_redemptions - comparisonAccount.total_redemptions,
        total_subscribers: currentAccount.total_subscribers - comparisonAccount.total_subscribers
      }
    };
  });

  return {
    period: 'vs_same_wtd_last_month',
    date_range: currentData.date_range,
    metrics: currentData.metrics, // Current WTD metrics
    comparison_date_range: comparisonData.date_range,
    comparison_metrics: comparisonData.metrics, // Last month WTD metrics
    accounts: accountsWithDeltas
  };
};

export const getSameWtdLastYearData = async () => {
  const calendarInfo = ChurnGuardCalendar.getDateInfo();
  const currentWeekStart = new Date(calendarInfo.week.start);
  const currentWeekEnd = new Date(calendarInfo.week.end);

  // Calculate how many days are in current WTD
  const currentDayCount = Math.floor((currentWeekEnd - currentWeekStart) / (1000 * 60 * 60 * 24)) + 1;

  // Get current WTD data
  const currentData = await getCurrentWeekData();

  // Go back 52 weeks
  const lastYearWeekStart = new Date(currentWeekStart);
  lastYearWeekStart.setDate(currentWeekStart.getDate() - (52 * 7));

  // Same number of days
  const lastYearWeekEnd = new Date(lastYearWeekStart);
  lastYearWeekEnd.setDate(lastYearWeekStart.getDate() + currentDayCount - 1);

  const weekStart = ChurnGuardCalendar.formatDateISO(lastYearWeekStart);
  const weekEnd = ChurnGuardCalendar.formatDateISO(lastYearWeekEnd);
  const currentMonth = ChurnGuardCalendar.getCurrentMonth();

  const comparisonData = await getWeeklyData(weekStart, weekEnd, currentMonth, 'Same WTD Last Year');

  // Create lookup map for comparison account data
  const comparisonAccountMap = new Map();
  comparisonData.accounts.forEach(account => {
    comparisonAccountMap.set(account.account_id, account);
  });

  // Add deltas to current accounts
  const accountsWithDeltas = currentData.accounts.map(currentAccount => {
    const comparisonAccount = comparisonAccountMap.get(currentAccount.account_id) || {
      total_spend: 0,
      total_texts: 0,
      total_redemptions: 0,
      total_subscribers: 0
    };

    return {
      ...currentAccount,
      deltas: {
        total_spend: currentAccount.total_spend - comparisonAccount.total_spend,
        total_texts: currentAccount.total_texts - comparisonAccount.total_texts,
        total_redemptions: currentAccount.total_redemptions - comparisonAccount.total_redemptions,
        total_subscribers: currentAccount.total_subscribers - comparisonAccount.total_subscribers
      }
    };
  });

  return {
    period: 'vs_same_wtd_last_year',
    date_range: currentData.date_range,
    metrics: currentData.metrics, // Current WTD metrics
    comparison_date_range: comparisonData.date_range,
    comparison_metrics: comparisonData.metrics, // Last year WTD metrics
    accounts: accountsWithDeltas
  };
};