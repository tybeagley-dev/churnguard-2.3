import { getSharedDatabase } from '../../config/database.js';
import { ChurnGuardCalendar } from '../utils/calendar.js';

const getAccountMetricsDataForMonthlyPeriod = async (startDate, endDate, eligibilityMonth, label = '') => {
  const db = await getSharedDatabase();

  console.log(`📊 Account Metrics Monthly - ${label}: ${startDate} to ${endDate} (eligibility: ${eligibilityMonth})`);

  const result = await db.query(`
    SELECT
      a.account_id,
      a.account_name,
      a.status,
      a.csm_owner,
      a.launched_at,

      -- Period totals from daily_metrics (MTD calculations)
      COALESCE(SUM(dm.total_spend), 0) as total_spend,
      COALESCE(SUM(dm.total_texts_delivered), 0) as total_texts_delivered,
      COALESCE(SUM(dm.coupons_redeemed), 0) as total_coupons_redeemed,
      COALESCE(ROUND(AVG(dm.active_subs_cnt)), 0) as total_subscribers

    FROM accounts a
    INNER JOIN monthly_metrics mm ON a.account_id = mm.account_id
      AND mm.month = $1
      AND COALESCE(mm.trending_risk_level, mm.historical_risk_level) IS NOT NULL
    LEFT JOIN daily_metrics dm ON a.account_id = dm.account_id
      AND dm.date >= $2 AND dm.date <= $3
    WHERE (
      -- Account eligibility: launched by eligibility period-end, not archived before eligibility period-start
      a.launched_at::date <= (($4 || '-01')::date + INTERVAL '1 month' - INTERVAL '1 day')
      AND (
        (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
        OR COALESCE(a.archived_at, a.earliest_unit_archived_at)::date >= ($5 || '-01')::date
      )
    )
    GROUP BY a.account_id, a.account_name, a.status, a.csm_owner, a.launched_at
    ORDER BY a.account_name ASC
  `, [eligibilityMonth, startDate, endDate, eligibilityMonth, eligibilityMonth]);
  const accounts = result.rows;

  // Calculate aggregated totals for summary cards
  const totals = accounts.reduce((acc, account) => {
    acc.total_spend += parseFloat(account.total_spend) || 0;
    acc.total_texts_delivered += parseInt(account.total_texts_delivered) || 0;
    acc.total_coupons_redeemed += parseInt(account.total_coupons_redeemed) || 0;
    acc.total_subscribers += parseInt(account.total_subscribers) || 0;
    return acc;
  }, {
    total_spend: 0,
    total_texts_delivered: 0,
    total_coupons_redeemed: 0,
    total_subscribers: 0
  });

  return {
    date_range: { start: startDate, end: endDate },
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

export const getCurrentMonthBaselineData = async () => {
  const db = await getSharedDatabase();
  const currentMonth = ChurnGuardCalendar.getCurrentMonth();
  const today = new Date();
  const startDate = `${currentMonth}-01`;
  const endDate = ChurnGuardCalendar.formatDateISO(new Date(today.getTime() - 24 * 60 * 60 * 1000)); // Yesterday (last complete day)

  console.log(`📊 Current Month Baseline: ${startDate} to ${endDate}`);

  // Get previous month for historical risk data
  const prevMonth = new Date();
  prevMonth.setMonth(prevMonth.getMonth() - 1);
  const prevMonthStr = prevMonth.toISOString().slice(0, 7);

  // Get accounts with monthly_metrics data (current MTD from ETL)
  const result = await db.query(`
    SELECT
      a.account_id,
      a.account_name,
      a.status,
      a.csm_owner,
      a.launched_at,

      -- Current month totals from monthly_metrics (updated daily by ETL)
      COALESCE(cm.total_spend, 0) as total_spend,
      COALESCE(cm.total_texts_delivered, 0) as total_texts_delivered,
      COALESCE(cm.total_coupons_redeemed, 0) as total_coupons_redeemed,
      COALESCE(ROUND(cm.avg_active_subs_cnt), 0) as total_subscribers,

      -- Current month trending risk data
      COALESCE(cm.trending_risk_level, 'low') as trending_risk_level,
      cm.trending_risk_reasons,

      -- Previous month historical risk data
      COALESCE(pm.historical_risk_level, 'low') as risk_level,
      pm.risk_reasons

    FROM accounts a
    INNER JOIN monthly_metrics cm ON a.account_id = cm.account_id
      AND cm.month = $1
      AND cm.trending_risk_level IS NOT NULL
    LEFT JOIN monthly_metrics pm ON a.account_id = pm.account_id
      AND pm.month = $2
    WHERE (
      -- Account eligibility
      a.launched_at::date <= (($3 || '-01')::date + INTERVAL '1 month' - INTERVAL '1 day')
      AND (
        (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
        OR COALESCE(a.archived_at, a.earliest_unit_archived_at)::date >= ($4 || '-01')::date
      )
    )
    ORDER BY a.account_name ASC
  `, [currentMonth, prevMonthStr, currentMonth, currentMonth]);
  const accounts = result.rows;

  // Calculate aggregated totals
  const totals = accounts.reduce((acc, account) => {
    acc.total_spend += parseFloat(account.total_spend) || 0;
    acc.total_texts_delivered += parseInt(account.total_texts_delivered) || 0;
    acc.total_coupons_redeemed += parseInt(account.total_coupons_redeemed) || 0;
    acc.total_subscribers += parseInt(account.total_subscribers) || 0;
    return acc;
  }, {
    total_spend: 0,
    total_texts_delivered: 0,
    total_coupons_redeemed: 0,
    total_subscribers: 0
  });

  return {
    date_range: { start: startDate, end: endDate },
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
      active_subs_cnt: account.total_subscribers,
      trending_risk_level: account.trending_risk_level,
      trending_risk_reasons: account.trending_risk_reasons ? JSON.parse(account.trending_risk_reasons) : ['No flags'],
      risk_level: account.risk_level,
      risk_reasons: account.risk_reasons ? JSON.parse(account.risk_reasons) : ['No flags']
    }))
  };
};

export const getMonthlyComparisonData = async (comparisonPeriod) => {
  const currentMonth = ChurnGuardCalendar.getCurrentMonth();
  const today = new Date();
  const currentDay = today.getDate();
  const lastCompleteDay = currentDay - 1; // Yesterday

  switch (comparisonPeriod) {
    case 'vs_previous_month': {
      const prevMonth = new Date();
      prevMonth.setMonth(prevMonth.getMonth() - 1);
      const prevMonthStr = prevMonth.toISOString().slice(0, 7);

      const startDate = `${prevMonthStr}-01`;
      const lastDayOfPrevMonth = new Date(prevMonth.getFullYear(), prevMonth.getMonth() + 1, 0).getDate();
      const endDay = Math.min(lastCompleteDay, lastDayOfPrevMonth);
      const endDate = `${prevMonthStr}-${endDay.toString().padStart(2, '0')}`;

      return await getAccountMetricsDataForMonthlyPeriod(
        startDate,
        endDate,
        prevMonthStr,
        'Previous Month MTD'
      );
    }

    case 'vs_3_month_avg': {
      const monthlyData = [];

      // Get data for each of the previous 3 months (same day range)
      for (let monthOffset = 1; monthOffset <= 3; monthOffset++) {
        const comparisonMonth = new Date();
        comparisonMonth.setMonth(comparisonMonth.getMonth() - monthOffset);
        const monthStr = comparisonMonth.toISOString().slice(0, 7);

        const startDate = `${monthStr}-01`;
        const lastDayOfMonth = new Date(comparisonMonth.getFullYear(), comparisonMonth.getMonth() + 1, 0).getDate();
        const endDay = Math.min(lastCompleteDay, lastDayOfMonth);
        const endDate = `${monthStr}-${endDay.toString().padStart(2, '0')}`;

        const data = await getAccountMetricsDataForMonthlyPeriod(
          startDate,
          endDate,
          monthStr,
          `Month ${monthOffset} ago MTD`
        );
        monthlyData.push(data);
      }

      // Calculate averages across the 3 months
      const accountAverageMap = new Map();

      monthlyData.forEach(monthData => {
        monthData.accounts.forEach(account => {
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
          avgAccount.total_spend += parseFloat(account.total_spend) || 0;
          avgAccount.total_texts_delivered += parseInt(account.total_texts_delivered) || 0;
          avgAccount.coupons_redeemed += parseInt(account.coupons_redeemed) || 0;
          avgAccount.active_subs_cnt += parseInt(account.active_subs_cnt) || 0;
        });
      });

      // Finalize averages (divide by 3)
      const averagedAccounts = Array.from(accountAverageMap.values()).map(account => ({
        ...account,
        total_spend: Math.round(account.total_spend / 3),
        total_texts_delivered: Math.round(account.total_texts_delivered / 3),
        coupons_redeemed: Math.round(account.coupons_redeemed / 3),
        active_subs_cnt: Math.round(account.active_subs_cnt / 3)
      }));

      // Calculate averaged metrics
      const avgMetrics = averagedAccounts.reduce((acc, account) => {
        acc.total_spend += parseFloat(account.total_spend) || 0;
        acc.total_texts += parseInt(account.total_texts_delivered) || 0;
        acc.total_redemptions += parseInt(account.coupons_redeemed) || 0;
        acc.total_subscribers += parseInt(account.active_subs_cnt) || 0;
        return acc;
      }, { total_spend: 0, total_texts: 0, total_redemptions: 0, total_subscribers: 0 });

      // Calculate date range for display
      const firstMonth = new Date();
      firstMonth.setMonth(firstMonth.getMonth() - 3);
      const thirdMonth = new Date();
      thirdMonth.setMonth(thirdMonth.getMonth() - 1);

      return {
        date_range: {
          start: `${firstMonth.toISOString().slice(0, 7)}-01`,
          end: `${thirdMonth.toISOString().slice(0, 7)}-${lastCompleteDay.toString().padStart(2, '0')}`
        },
        metrics: avgMetrics,
        accounts: averagedAccounts
      };
    }

    case 'vs_same_month_last_year': {
      const lastYearMonth = new Date();
      lastYearMonth.setFullYear(lastYearMonth.getFullYear() - 1);
      const lastYearMonthStr = lastYearMonth.toISOString().slice(0, 7);

      const startDate = `${lastYearMonthStr}-01`;
      const lastDayOfMonth = new Date(lastYearMonth.getFullYear(), lastYearMonth.getMonth() + 1, 0).getDate();
      const endDay = Math.min(lastCompleteDay, lastDayOfMonth);
      const endDate = `${lastYearMonthStr}-${endDay.toString().padStart(2, '0')}`;

      return await getAccountMetricsDataForMonthlyPeriod(
        startDate,
        endDate,
        lastYearMonthStr,
        'Same Month Last Year MTD'
      );
    }

    default:
      throw new Error(`Unsupported comparison period: ${comparisonPeriod}`);
  }
};

export const getRiskLevelCounts = async () => {
  const db = await getSharedDatabase();
  const currentMonth = ChurnGuardCalendar.getCurrentMonth();

  // Get previous month for historical risk data
  const prevMonth = new Date();
  prevMonth.setMonth(prevMonth.getMonth() - 1);
  const prevMonthStr = prevMonth.toISOString().slice(0, 7);

  // Get risk level counts for eligible accounts
  const countsResult = await db.query(`
    SELECT
      -- Current month trending risk counts
      SUM(CASE WHEN mm.trending_risk_level = 'high' THEN 1 ELSE 0 END) as trending_high,
      SUM(CASE WHEN mm.trending_risk_level = 'medium' THEN 1 ELSE 0 END) as trending_medium,
      SUM(CASE WHEN mm.trending_risk_level = 'low' THEN 1 ELSE 0 END) as trending_low,

      -- Previous month historical risk counts
      SUM(CASE WHEN pm.historical_risk_level = 'high' THEN 1 ELSE 0 END) as historical_high,
      SUM(CASE WHEN pm.historical_risk_level = 'medium' THEN 1 ELSE 0 END) as historical_medium,
      SUM(CASE WHEN pm.historical_risk_level = 'low' THEN 1 ELSE 0 END) as historical_low

    FROM accounts a
    INNER JOIN monthly_metrics mm ON a.account_id = mm.account_id
      AND mm.month = $1
      AND mm.trending_risk_level IS NOT NULL
    LEFT JOIN monthly_metrics pm ON a.account_id = pm.account_id
      AND pm.month = $2
    WHERE (
      -- Account eligibility for current month
      a.launched_at::date <= (($3 || '-01')::date + INTERVAL '1 month' - INTERVAL '1 day')
      AND (
        (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
        OR COALESCE(a.archived_at, a.earliest_unit_archived_at)::date >= ($4 || '-01')::date
      )
    )
  `, [currentMonth, prevMonthStr, currentMonth, currentMonth]);
  const counts = countsResult.rows;

  const countsData = counts[0] || {
    trending_high: 0, trending_medium: 0, trending_low: 0,
    historical_high: 0, historical_medium: 0, historical_low: 0
  };

  return {
    trending: {
      high: countsData.trending_high,
      medium: countsData.trending_medium,
      low: countsData.trending_low
    },
    historical: {
      high: countsData.historical_high,
      medium: countsData.historical_medium,
      low: countsData.historical_low
    }
  };
};

export const calculateAccountDeltas = (baselineAccounts, comparisonAccounts) => {
  // Create lookup maps for both datasets
  const baselineMap = new Map();
  baselineAccounts.forEach(account => {
    baselineMap.set(account.account_id, account);
  });

  const comparisonMap = new Map();
  comparisonAccounts.forEach(account => {
    comparisonMap.set(account.account_id, account);
  });

  // Create union of all account IDs
  const allAccountIds = new Set([
    ...baselineMap.keys(),
    ...comparisonMap.keys()
  ]);

  // Build unified account list with deltas and status labels
  return Array.from(allAccountIds).map(accountId => {
    const baselineAccount = baselineMap.get(accountId);
    const comparisonAccount = comparisonMap.get(accountId);

    // Determine status label
    let statusLabel = null;
    if (!comparisonAccount && baselineAccount) {
      statusLabel = '🟢 Current Period Only';
    } else if (comparisonAccount && !baselineAccount) {
      statusLabel = '🔴 Comparison Period Only';
    }

    // Use baseline account data if available, otherwise comparison account data
    const baseAccount = baselineAccount || comparisonAccount;
    const baselineMetrics = baselineAccount || {
      total_spend: 0,
      total_texts_delivered: 0,
      coupons_redeemed: 0,
      active_subs_cnt: 0
    };
    const comparisonMetrics = comparisonAccount || {
      total_spend: 0,
      total_texts_delivered: 0,
      coupons_redeemed: 0,
      active_subs_cnt: 0
    };

    return {
      ...baseAccount,
      total_spend: baselineMetrics.total_spend,
      total_texts_delivered: baselineMetrics.total_texts_delivered,
      coupons_redeemed: baselineMetrics.coupons_redeemed,
      active_subs_cnt: baselineMetrics.active_subs_cnt,
      status_label: statusLabel,
      spend_delta: baselineMetrics.total_spend - comparisonMetrics.total_spend,
      texts_delta: baselineMetrics.total_texts_delivered - comparisonMetrics.total_texts_delivered,
      coupons_delta: baselineMetrics.coupons_redeemed - comparisonMetrics.coupons_redeemed,
      subs_delta: baselineMetrics.active_subs_cnt - comparisonMetrics.active_subs_cnt
    };
  }).sort((a, b) => a.name.localeCompare(b.name));
};