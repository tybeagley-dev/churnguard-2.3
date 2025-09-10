# ChurnGuard 2.3

**GitHub Repository:** https://github.com/tybeagley-dev/churnguard-2.3  
**Current Version:** 2.3  
**Date:** September 9, 2025

### 🚀 Quick Start
```bash
npm install
npm run dev
# Access at http://localhost:3002
```

## 🔄 Recovery Instructions

**To recover this stable version from anywhere:**
```bash
git clone https://github.com/tybeagley-dev/churnguard-2.3.git
cd churnguard-2.3
npm install
npm run dev
# Access at http://localhost:3002
```

**Current Status:**
- ✅ 49 clean files (no sensitive data, no databases)
- ✅ Complete stable working version saved to GitHub
- ✅ Frontend with diagonal stripes working
- ✅ All API endpoints functional  
- ✅ 873 accounts with proper risk distributions

---

## What Works in This Version

### ✅ Frontend
- Clean UI without git conflict markers
- Monthly trends chart with diagonal stripes for current month
- All charts rendering properly
- 873 accounts with current risk distributions

### ✅ Backend API  
- All endpoints functional
- Database: SQLite simulation 
- Risk distributions: 163 high, 531 medium, 179 low risk

### ✅ Key Features
- Dashboard with live data
- Historical performance tracking
- Account risk assessment
- Monthly trends with current month highlighting

---

## Original Development Info

**DEVELOPMENT DIRECTORY** - This is the primary development directory. All development work happens here.

This repo simulates a PostgreSQL-based ChurnGuard architecture using SQLite as the database. It extracts data from your existing BigQuery setup and structures it like a PostgreSQL database would for optimal performance.

## Development Workflow

⚠️ **IMPORTANT**: This is the primary development environment for ChurnGuard 2.3.

- **Single repo approach**: All development happens here
- **GitHub management**: Uses .gitignore to manage file size limits
- **Direct deployment**: Changes are committed and pushed directly to GitHub

## Purpose

Test the PostgreSQL approach before building the real thing:
- Daily ETL scripts that mirror what a real PostgreSQL system would do
- Separate scripts for each metric (spend, texts, coupons, subscribers)
- Multiple scripts can populate the same row (account_id + date)
- Parallel execution for speed

## Setup

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Database setup:**
   - Uses SQLite database for development
   - Database file is managed via .gitignore for size control

3. **Verify setup:**
   ```bash
   npm run dev
   ```

## Running the Simulation

### Full Simulation (Recommended)
Simulates July 1, 2025 through September 3, 2025:
```bash
npm run simulate-all
```
This will:
1. Populate the accounts table
2. Run daily ETL for each day (65 days total)
3. Show progress and timing

### Individual Components

**Set up accounts table:**
```bash
npm run setup-accounts
```

**Simulate a single day:**
```bash
npm run simulate-day 2025-07-01
```

**Run daily simulation manually:**
```bash
node scripts/run-daily-simulation.js 2025-07-15
```

## Expected Timing

- **Full simulation**: ~27 minutes (conservative estimate)
- **Per day**: ~25 seconds (4 parallel queries + database updates)
- **Optimistic**: ~11 minutes if queries are fast

## Database Structure

**Table: accounts**
- account_id, account_name, status, launched_at, csm_owner, hubspot_id, archived_at, last_updated

**Table: daily_metrics**
- account_id, date, total_spend, total_texts_delivered, coupons_redeemed, active_subs_cnt
- Plus timestamp columns for tracking when each metric was last updated

## Next Steps

After simulation completes:
1. Analyze the SQLite database structure
2. Test building aggregations and dashboard queries
3. Measure query performance vs current BigQuery approach
4. Use this as the blueprint for real PostgreSQL implementation