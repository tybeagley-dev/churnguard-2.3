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
      risk_level = null,
      status = null,
      csm_owner = null
    } = req.query;

    // Handle multiple CSM owners (can be array if multiple params sent)
    const csmOwners = Array.isArray(csm_owner) ? csm_owner : (csm_owner ? [csm_owner] : null);

    console.log(`📊 Account Metrics Monthly: baseline=${baseline}, comparison=${comparison}, status=${status}, csm_owner=${csmOwners?.join(',') || 'all'}`);

    // Create filters object for service functions
    const filters = { status, csm_owner: csmOwners, risk_level };

    // Always get current month baseline with risk data and filters
    const baselineData = await getCurrentMonthBaselineData(filters);

    // Get risk level counts for summary cards (filtered)
    const riskCounts = await getRiskLevelCounts(filters);

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

    // Get comparison period data with same filters applied
    const comparisonData = await getMonthlyComparisonData(comparison, filters);

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
      accounts: accountsWithDeltas,
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