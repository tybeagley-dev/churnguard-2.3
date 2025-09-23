import {
  getCurrentMonthBaselineData,
  getMonthlyComparisonData,
  calculateAccountDeltas,
  getRiskLevelCounts
} from '../services/account-metrics-monthly.service.js';

export const getAccountMetricsMonthly = async (req, res) => {
  try {
    const {
      baseline = 'current_month',
      comparison = null,
      risk_level = null
    } = req.query;


    // Always get current month baseline with risk data
    const baselineData = await getCurrentMonthBaselineData();

    // Get risk level counts for summary cards
    const riskCounts = await getRiskLevelCounts();

    // If no comparison requested, return baseline only with risk counts
    if (!comparison) {
      return res.json({
        baseline: {
          period: baseline,
          ...baselineData
        },
        risk_counts: riskCounts
      });
    }

    // Get comparison period data
    const comparisonData = await getMonthlyComparisonData(comparison);

    // Calculate deltas between baseline and comparison
    const accountsWithDeltas = calculateAccountDeltas(baselineData.accounts, comparisonData.accounts);

    // Apply risk level filtering if requested
    let filteredAccounts = accountsWithDeltas;
    if (risk_level) {
      // Filter accounts by trending risk level
      filteredAccounts = accountsWithDeltas.filter(account => {
        return account.trending_risk_level === risk_level;
      });
    }

    const response = {
      baseline: {
        period: baseline,
        date_range: baselineData.date_range,
        metrics: baselineData.metrics
      },
      comparison: {
        period: comparison,
        date_range: comparisonData.date_range,
        metrics: comparisonData.metrics
      },
      accounts: filteredAccounts,
      risk_counts: riskCounts
    };

    res.json(response);

  } catch (error) {
    console.error('Error fetching account metrics monthly:', error);
    res.status(500).json({
      error: 'Failed to fetch account metrics monthly',
      details: error.message
    });
  }
};