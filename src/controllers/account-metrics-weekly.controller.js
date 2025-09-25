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
      risk_level = null,
      status = null,
      csm_owner = null
    } = req.query;

    console.log(`📊 Account Metrics Overview: baseline=${baseline}, comparison=${comparison}, status=${status}, csm_owner=${csm_owner}`);

    // Create filters object for service functions
    const filters = { status, csm_owner, risk_level };

    // Always get current week baseline with filters applied
    const baselineData = await getCurrentWeekBaselineData(filters);

    // If no comparison requested, return baseline only
    if (!comparison) {
      return res.json({
        baseline: {
          period: baseline,
          ...baselineData
        }
      });
    }

    // Get comparison period data with same filters applied
    const comparisonData = await getComparisonData(comparison, filters);

    // Calculate deltas between baseline and comparison
    const accountsWithDeltas = calculateAccountDeltas(baselineData.accounts, comparisonData.accounts);

    // Note: Filtering is now handled in service functions, so accounts are already filtered

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
      accounts: accountsWithDeltas
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