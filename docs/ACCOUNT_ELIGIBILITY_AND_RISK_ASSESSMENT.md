# ChurnGuard Account Eligibility and Risk Assessment System

## Overview
This document defines the **backbone** of the ChurnGuard system - the eligibility criteria and risk assessment algorithms that determine which accounts are included in analysis and how their churn risk is calculated.

## Account Eligibility Criteria

All ChurnGuard analyses use **consistent eligibility filtering** based on the `accounts` table:

### Core Requirements
1. **Launch Status**: Must have `launched_at` date (not NULL)
2. **Launch Timing**: Must be launched by the end of the analysis period
3. **Archive Status**: Must not be archived before the analysis period starts

### SQL Implementation
```sql
WHERE (
  -- Account eligibility: launched by period-end, not archived before period-start
  DATE(a.launched_at) <= DATE(period || '-01', '+1 month', '-1 day')
  AND (
    (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
    OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(period || '-01')
  )
)
```

### Archive Date Logic
- **Before Nov 2024**: `archived_at` contains valid data
- **After Nov 2024**: `earliest_unit_archived_at` is source of truth due to internal process change
- **Implementation**: `COALESCE(archived_at, earliest_unit_archived_at)` handles both periods

## Risk Assessment System

ChurnGuard uses **two distinct risk assessment approaches**:

### 1. Historical Risk Level (Completed Months)
**Script**: `populate-historical-risk-levels.js`
**Field**: `monthly_metrics.historical_risk_level`
**Applied to**: All months except current month

#### Algorithm
**Immediate High Risk:**
- Archived during the specific month
- FROZEN status with no text messages sent

**Flag-Based System for LAUNCHED accounts:**
- **Flag 1** (1 point): Monthly redemptions < 10
- **Flag 2** (2 points): Low engagement combo - < 300 subs AND < 35 redemptions *(after month 2)*
- **Flag 3** (1 point): Low activity - < 300 subscribers
- **Flag 4** (1 point): Spend drop ≥ 40% from previous month *(after month 3)*
- **Flag 5** (1 point): Redemptions drop ≥ 50% from previous month *(after month 3)*

**Risk Levels:**
- **≥ 3 flags**: High risk
- **≥ 1 flag**: Medium risk
- **0 flags**: Low risk

### 2. Trending Risk Level (Current Month)
**Script**: `daily-production-etl.js`
**Field**: `monthly_metrics.trending_risk_level`
**Applied to**: Current month only

#### Algorithm
Uses the same flag-based system as historical but with **real-time projections**:

**Proportional Calculations:**
- Progress percentage: `(dayOfMonth - 1) / daysInMonth`
- Proportional redemptions threshold: `10 * progressPercentage`
- Proportional low engagement threshold: `35 * progressPercentage`

**Same-Day Comparisons:**
- Flags 7 & 8 compare current month-to-date with previous month's same-day totals (apples-to-apples)

**8-Flag Extended System:**
- Flags 1-6: Same as historical with proportional thresholds
- **Flag 7** (1 point): Spend drop ≥ 40% vs same day previous month
- **Flag 8** (1 point): Redemptions drop ≥ 50% vs same day previous month

## Service Implementation

All ChurnGuard services use the **COALESCE pattern** for risk level selection:

```sql
COALESCE(mm.trending_risk_level, mm.historical_risk_level)
```

This ensures:
- **Current month**: Uses `trending_risk_level` (real-time assessment)
- **Completed months**: Uses `historical_risk_level` (comprehensive assessment)

## ETL Pipeline Overview

### Data Flow
1. **BigQuery → accounts table** (via `accounts-etl-sqlite.js`)
2. **BigQuery → daily_metrics** (via `daily-*-etl-sqlite.js` scripts)
3. **daily_metrics → monthly_metrics** (aggregated monthly via `update-current-month.js`)
4. **Risk Assessment**:
   - Historical: `populate-historical-risk-levels.js`
   - Trending: `daily-production-etl.js`

### Clean Separation
- **`update-current-month.js`**: Creates current month records WITHOUT risk assessment
- **`populate-historical-risk-levels.js`**: Calculates `historical_risk_level` for completed months
- **`daily-production-etl.js`**: Calculates `trending_risk_level` for current month

## Key Thresholds

| Metric | Threshold | Purpose |
|--------|-----------|---------|
| Monthly Redemptions | 10 | Minimum acceptable engagement |
| Low Activity Subscribers | 300 | Minimum viable audience size |
| Low Engagement Combo | 300 subs + 35 redemptions | Critical engagement floor |
| Spend Drop | 40% | Significant revenue decline |
| Redemptions Drop | 50% | Major engagement decline |
| Account Maturity (Enhanced Flags) | 3 months | Required for month-over-month comparisons |

## Status-Based Rules

| Account Status | Risk Level | Condition |
|----------------|------------|-----------|
| ARCHIVED | High | If archived during analysis period |
| FROZEN + No Texts | High | No activity in current period |
| FROZEN + Has Texts | Medium | Limited but present activity |
| LAUNCHED | Flag-based | Uses comprehensive flag system |

---

**This system ensures consistent, data-driven churn risk assessment across all ChurnGuard features while maintaining real-time trending capabilities for current month analysis.**