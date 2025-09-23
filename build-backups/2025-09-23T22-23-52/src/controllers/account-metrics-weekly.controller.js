import {
  getCurrentWeekBaselineData,
  getComparisonData,
  calculateAccountDeltas
} from '../services/account-metrics-weekly.service.js';

export const getAccountMetricsOverview = async (req, res) => {
  try {
    const {
      baseline = 'current_week',
      comparison = null,
      risk_level = null
    } = req.query;

    console.log(`📊 Account Metrics Overview: baseline=${baseline}, comparison=${comparison}`);

    // Always get current week baseline (consistent across all comparisons)
    const baselineData = await getCurrentWeekBaselineData();

    // If no comparison requested, return baseline only
    if (!comparison) {
      return res.json({
        baseline: {
          period: baseline,
          ...baselineData
        }
      });
    }

    // Get comparison period data
    const comparisonData = await getComparisonData(comparison);

    // Calculate deltas between baseline and comparison
    const accountsWithDeltas = calculateAccountDeltas(baselineData.accounts, comparisonData.accounts);

    // Apply risk level filtering if requested
    let filteredAccounts = accountsWithDeltas;
    if (risk_level) {
      // TODO: Implement risk level filtering logic
      console.log(`🎯 Filtering by risk level: ${risk_level}`);
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
      accounts: filteredAccounts
    };

    res.json(response);

  } catch (error) {
    console.error('Error fetching account metrics overview:', error);
    res.status(500).json({
      error: 'Failed to fetch account metrics overview',
      details: error.message
    });
  }
};