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

**Complete 8-Flag System:**

**Flags 1-5 (LAUNCHED accounts only):**
- **Flag 1** (1 point): Monthly redemptions < 10
- **Flag 2** (2 points): Low engagement combo - < 300 subs AND < 35 redemptions *(after month 2)*
- **Flag 3** (1 point): Low activity - < 300 subscribers
- **Flag 4** (1 point): Spend drop ≥ 40% from previous month *(after month 3)*
- **Flag 5** (1 point): Redemptions drop ≥ 50% from previous month *(after month 3)*

**Flags 6-8 (Status-based, automatic assignment):**
- **Flag 6**: FROZEN status with text messages sent → Medium risk
- **Flag 7**: FROZEN status with no text messages sent → High risk
- **Flag 8**: ARCHIVED during analysis period → High risk

**Risk Levels:**
- **≥ 3 flags**: High risk
- **≥ 1 flag**: Medium risk
- **0 flags**: Low risk

### 2. Trending Risk Level (Current Month)
**Script**: `daily-production-etl.js`
**Field**: `monthly_metrics.trending_risk_level`
**Applied to**: Current month only

#### Algorithm
Uses the same 8-flag system as historical but with **month-to-date time windows**:

**For Flags 1-3 (Proportional Calculations):**
- Progress percentage: `(dayOfMonth - 1) / daysInMonth`
- Proportional redemptions threshold: `10 * progressPercentage`
- Proportional low engagement threshold: `35 * progressPercentage`

**For Flags 4-5 (MTD Comparisons):**
- Direct comparison of current month-to-date vs previous month same-day totals
- **Flag 4**: Spend drop ≥ 40% (current MTD vs previous month same days)
- **Flag 5**: Redemptions drop ≥ 50% (current MTD vs previous month same days)

**For Flags 6-8 (Status-Based):**
- Applied immediately based on account status (same as historical)

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

| Account Status | Flag Applied | Risk Level | Processing |
|----------------|--------------|------------|-----------|
| ARCHIVED | Flag 8 | High | Skip Flags 1-7 |
| FROZEN + No Texts | Flag 7 | High | Skip Flags 1-6 |
| FROZEN + Has Texts | Flag 6 | Medium | Skip Flags 1-5 |
| LAUNCHED | Flags 1-5 | Count-based | Evaluate all applicable flags |

---

**This system ensures consistent, data-driven churn risk assessment across all ChurnGuard features while maintaining real-time trending capabilities for current month analysis.**