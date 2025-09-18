# Comparison Period Eligibility Enhancement

**Date**: September 17, 2025
**Status**: Documented (Not Yet Implemented)
**Component**: Weekly View Service

## Overview

Enhancement proposal to address asymmetric account sets in weekly view comparison periods, providing complete historical business intelligence instead of current-account-only performance tracking.

## Current Problem

Weekly View comparison periods (Previous WTD, 6-Week Average, Same WTD Last Month, etc.) currently use **current month eligibility** for all time periods, causing:

- **Historical blind spots**: Missing accounts that churned between comparison period and now
- **Incomplete trend analysis**: Can't see full performance picture for past periods
- **Understated metrics**: Historical totals appear lower than reality
- **Asymmetric comparisons**: Only shows deltas for accounts eligible in current month

### Example Issue
- August 2025: 905 eligible accounts
- September 2025: 888 eligible accounts
- **17 accounts missing** from historical comparisons (likely churned in September)

## Proposed Solution: Historical Eligibility

### Enhanced `getWeeklyData()` Function

```javascript
const getWeeklyData = async (weekStart, weekEnd, month, label = '', useHistoricalEligibility = false) => {
  const eligibilityMonth = useHistoricalEligibility
    ? deriveMonthFromDateRange(weekStart, weekEnd)  // Calculate appropriate month
    : month; // Current month (existing behavior)
}
```

### Month Derivation Logic

```javascript
const deriveMonthFromDateRange = (weekStart, weekEnd) => {
  // Use the end date month for consistency
  const endDate = new Date(weekEnd);
  return `${endDate.getFullYear()}-${String(endDate.getMonth() + 1).padStart(2, '0')}`;
}
```

### Asymmetric Account Set Handling

```javascript
// Get union of account IDs from both periods
const allAccountIds = new Set([
  ...currentData.accounts.map(a => a.account_id),
  ...comparisonData.accounts.map(a => a.account_id)
]);

// Handle three scenarios:
const accountsWithDeltas = Array.from(allAccountIds).map(accountId => {
  const currentAccount = currentAccountMap.get(accountId);
  const comparisonAccount = comparisonAccountMap.get(accountId);

  // 1. Account exists in both periods → Normal delta calculation
  // 2. Account only in current → Show as "NEW" with zero comparison baseline
  // 3. Account only in comparison → Show as "CHURNED" with zero current values

  return {
    account_id: accountId,
    account_name: currentAccount?.account_name || comparisonAccount?.account_name,
    status_current: currentAccount?.status || 'NOT_ELIGIBLE_CURRENT',
    status_comparison: comparisonAccount?.status || 'NOT_ELIGIBLE_COMPARISON',
    deltas: {
      total_spend: currentMetrics.total_spend - comparisonMetrics.total_spend,
      // ... other metrics
    }
  };
});
```

## Implementation Details

### Call Site Updates

**Current:**
```javascript
const comparisonData = await getWeeklyData(weekStart, weekEnd, currentMonth, 'Previous WTD');
```

**Enhanced:**
```javascript
const comparisonData = await getWeeklyData(weekStart, weekEnd, currentMonth, 'Previous WTD', true);
```

### Functions to Update

1. `getPreviousWtdData()` - Previous week comparison
2. `getPrevious6WeekAvgData()` - 6-week average comparison
3. `getSameWtdLastMonthData()` - Same week last month comparison
4. `getSameWtdLastYearData()` - Same week last year comparison

### Helper Function

```javascript
const buildAccountsWithDeltas = (currentData, comparisonData) => {
  // Comprehensive account union and delta calculation
  // Handles missing accounts with appropriate status indicators
}
```

## Impact Analysis

### Data Accuracy Improvements

**Before**: Sept 2025 vs Aug 2025 comparison
- Only shows accounts eligible in Sept 2025
- Missing accounts that churned in September

**After**: Sept 2025 vs Aug 2025 comparison
- Shows all accounts eligible in either period
- Reveals churn impact on metrics
- More accurate historical performance picture

### UI Considerations

You'd need to handle these display scenarios:

1. **Standard account**: Exists in both periods → Normal delta display
2. **New account**: Only in current → Show as "NEW" with zero comparison baseline
3. **Churned account**: Only in comparison → Show as "CHURNED" with zero current values

### Performance Impact

**Minimal**: Same query structure, just different month parameters. Potentially slightly larger result sets but not significantly.

## Implementation Phases

**Phase 1**: Add flag to `getWeeklyData()`, default `false` (no behavior change)
**Phase 2**: Update comparison functions to use historical eligibility
**Phase 3**: Enhance UI to handle asymmetric account sets
**Phase 4**: Add toggle for users to switch between modes

## Benefits

- **Complete historical analysis**: No more missing churned accounts in comparisons
- **Accurate trend detection**: Full business intelligence for churn patterns
- **Better decision making**: See true impact of account changes over time
- **Backward compatibility**: Existing behavior preserved with flag system

## Key Insight

This enhancement transforms the weekly view from "current account performance tracking" to "complete historical business intelligence" - significantly more valuable for trend analysis and churn detection.

The change reveals the full story of business performance, including accounts that may no longer be eligible but were active during comparison periods.