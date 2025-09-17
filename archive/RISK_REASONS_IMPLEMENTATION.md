# Risk Reasons Database Implementation Plan

## Overview
Move risk reason calculation from frontend to database storage for better performance, consistency, and completeness. This addresses the incomplete risk reason filtering in Monthly View where only basic flags are available.

## Current State
- **Frontend Issue**: Only shows "Frozen Account Status", "Low Activity", "Low Monthly Redemptions" and "No flags"
- **Missing Reasons**: Low Engagement Combo, Recently Archived, Frozen & Inactive, Spend Drop, Redemptions Drop
- **Root Cause**: Frontend calculates reasons from limited flag data instead of using complete 8-flag system

## Proposed Solution

### 1. Schema Changes
```sql
ALTER TABLE monthly_metrics ADD COLUMN risk_reasons TEXT; 
ALTER TABLE monthly_metrics ADD COLUMN trending_risk_reasons TEXT;
```

**Data Format**: JSON Array
```json
["Low Monthly Redemptions", "Spend Drop", "Low Activity"]
```

### 2. 8-Flag Risk System Reasons
Based on `populate-historical-risk-levels.js` and `daily-production-etl.js`:

#### Historical Risk Reasons (Completed Months)
1. **"Recently Archived"** - Account archived during specific month
2. **"Frozen Account Status"** - Account status is FROZEN
3. **"Frozen & Inactive"** - FROZEN + no texts in month  
4. **"Low Monthly Redemptions"** - < 10 redemptions/month
5. **"Low Engagement Combo"** - < 300 subs AND < 35 redemptions (after month 2)
6. **"Low Activity"** - < 300 subscribers  
7. **"Spend Drop"** - ≥ 40% decrease from previous month
8. **"Redemptions Drop"** - ≥ 50% decrease from previous month

#### Trending Risk Reasons (Current Month)
- Same 8 flags but applied to:
  - **Projected values** for flags 4-6 (redemptions, engagement, activity)
  - **Same-day comparisons** for flags 7-8 (spend drop, redemptions drop)

### 3. Update Schedule

#### Historical Months (month_status = 'complete')
- **Manual population**: Run once now for existing data  
- **Automated**: Updated once at start of subsequent month via ETL
- **Immutable**: Never changed after completion

#### Current Month (month_status = 'current') 
- **Daily updates**: Via `daily-production-etl.js` 
- **Dynamic**: Recalculated each day with latest data
- **Trending logic**: Uses proper 8-flag system with same-day comparisons

### 4. Implementation Steps

#### Step 1: Schema Migration
```bash
sqlite3 ./data/churnguard_simulation.db << 'EOF'
ALTER TABLE monthly_metrics ADD COLUMN risk_reasons TEXT;
ALTER TABLE monthly_metrics ADD COLUMN trending_risk_reasons TEXT; 
EOF
```

#### Step 2: Historical Data Population
Update `populate-historical-risk-levels.js`:
- Modify `calculateRiskLevel()` to return `{level, reasons}`
- Store reasons as JSON array in `risk_reasons` column
- Populate all existing historical data

#### Step 3: Daily ETL Enhancement  
Update `daily-production-etl.js`:
- Modify `calculateTrendingRiskLevel()` to return `{level, reasons}`
- Store trending reasons as JSON array in `trending_risk_reasons` column
- Update daily for current month data

#### Step 4: API Enhancement
Update `/api/monthly-trends` and related endpoints:
- Return parsed `risk_reasons` and `trending_risk_reasons` arrays
- Remove frontend risk calculation dependency

#### Step 5: Frontend Update
Update `account-metrics-table-monthly.tsx`:
- Replace `getRiskReasons()` with direct database field access
- Use parsed JSON arrays for filtering
- Remove complex frontend logic

### 5. Risk Level Priority Logic
```javascript
// For filtering, use appropriate reasons based on time period
const getRiskReasons = (account, isForTrending = false) => {
  if (isForTrending && account.trending_risk_reasons) {
    return JSON.parse(account.trending_risk_reasons);
  } else if (account.risk_reasons) {
    return JSON.parse(account.risk_reasons);  
  }
  return ['No flags'];
};
```

### 6. Expected Outcomes
- **Complete risk reason filtering** - All 8 flags available in Monthly View
- **Better performance** - No frontend calculation overhead
- **Consistent reasoning** - Same logic across historical and trending
- **Accurate filtering** - Proper same-day comparisons for drop calculations
- **Fixed UI labels** - "All Selected" instead of "All CSMs"

### 7. Testing Strategy
1. **Schema migration** - Verify columns added successfully
2. **Historical population** - Check all historical months get reasons
3. **Daily ETL** - Verify trending reasons update daily for current month
4. **API response** - Confirm endpoints return parsed reason arrays
5. **Frontend filtering** - Test all 8 risk reasons appear and filter correctly

### 8. Rollback Plan
- Remove new columns if needed: `ALTER TABLE monthly_metrics DROP COLUMN risk_reasons;`
- Frontend falls back to existing `getRiskReasons()` function
- No data loss - risk levels remain intact

## Files to Modify
1. **Database**: `monthly_metrics` table schema
2. **Backend**: `populate-historical-risk-levels.js`, `daily-production-etl.js` 
3. **API**: `server.js` endpoints (`/api/monthly-trends`, etc.)
4. **Frontend**: `account-metrics-table-monthly.tsx`, `multi-select.tsx` (✅ completed)
5. **Types**: `AccountMetric` interface

## Benefits
- ✅ **Complete 8-flag system** in Monthly View filters
- ✅ **Proper same-day drop comparisons** 
- ✅ **Database-driven consistency** across all views
- ✅ **Performance improvement** via elimination of frontend calculation
- ✅ **Easier maintenance** - single source of truth for risk reasoning