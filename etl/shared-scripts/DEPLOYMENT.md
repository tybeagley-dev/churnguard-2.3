# ChurnGuard ETL Deployment Guide

## Overview

The ChurnGuard ETL system consists of three main components:
- **Daily Metrics ETL**: Processes BigQuery data into PostgreSQL daily_metrics table
- **Monthly Rollup ETL**: Aggregates daily data into monthly_metrics with trending risk calculations
- **Cron Manager**: Orchestrates automated execution with error handling and logging

## Production Deployment Options

### Option 1: Local Cron Scheduling (Development/Local Server)

1. **Install crontab configuration:**
   ```bash
   cd /Users/tylerbeagley/Boostly/ai/projects/ChurnGuard/churnguard-v2.3-repo
   crontab etl/shared-scripts/production-crontab.txt
   ```

2. **Verify cron installation:**
   ```bash
   crontab -l
   ```

3. **Create log directory:**
   ```bash
   sudo mkdir -p /var/log
   sudo touch /var/log/churnguard-etl.log
   sudo chmod 666 /var/log/churnguard-etl.log
   ```

4. **Monitor execution:**
   ```bash
   tail -f /var/log/churnguard-etl.log
   ```

### Option 2: Render Web Service Cron (Recommended for Production)

1. **Use Render's built-in cron jobs:**
   - Navigate to Render Dashboard → Web Service → Cron Jobs
   - Add daily job: `0 6 * * * cd /opt/render/project/src && node etl/shared-scripts/cron-manager.js daily`
   - Add monthly job: `0 7 1 * * cd /opt/render/project/src && node etl/shared-scripts/cron-manager.js monthly`

2. **Or use API-based scheduling with external service:**
   - Set up external cron service (like GitHub Actions, AWS EventBridge)
   - Make HTTP POST to: `https://your-app.onrender.com/api/admin/sync-data`
   - Schedule daily at 6:00 AM UTC

### Option 3: API-Based Execution (Manual/External Triggers)

Execute ETL processes via API endpoints:

```bash
# Complete daily pipeline
curl -X POST https://your-app.onrender.com/api/admin/sync-data \
  -H "Content-Type: application/json" \
  -d '{"date": "2025-09-24"}'

# Individual steps
curl -X POST https://your-app.onrender.com/api/admin/sync-accounts
curl -X POST https://your-app.onrender.com/api/admin/sync-daily
curl -X POST https://your-app.onrender.com/api/admin/sync-monthly
```

## Manual Execution

### Command Line Interface

```bash
# Daily ETL (processes yesterday by default)
node etl/shared-scripts/cron-manager.js daily

# Daily ETL for specific date
node etl/shared-scripts/cron-manager.js daily 2025-09-24

# Monthly rollup (current month by default)
node etl/shared-scripts/cron-manager.js monthly

# Monthly rollup for specific month
node etl/shared-scripts/cron-manager.js monthly 2025-09

# Test connections and dry run
node etl/shared-scripts/cron-manager.js test

# Full pipeline (daily + monthly if 1st of month)
node etl/shared-scripts/cron-manager.js full
```

### Direct ETL Execution

```bash
# Daily metrics ETL
node etl/postgresql-native/daily-metrics-etl-postgres-native.js 2025-09-24

# Daily metrics ETL (dry run)
node etl/postgresql-native/daily-metrics-etl-postgres-native.js 2025-09-24 --dry-run

# Monthly rollup ETL
node etl/postgresql-native/monthly-rollup-etl-postgres-native.js 2025-09

# Test PostgreSQL connection
node etl/postgresql-native/daily-metrics-etl-postgres-native.js --test-connection
```

## Environment Configuration

Ensure the following environment variables are set:

```bash
# PostgreSQL Connection
DATABASE_URL=postgresql://user:password@host:port/database
# OR
EXTERNAL_DATABASE_URL=postgresql://user:password@host:port/database

# BigQuery Configuration
GOOGLE_CLOUD_PROJECT_ID=your-project-id
GOOGLE_APPLICATION_CREDENTIALS_JSON='{...}' # JSON string
# OR
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json # File path

# Optional: Logging and Notifications
ETL_LOG_LEVEL=info # debug|info|warn|error
SLACK_WEBHOOK_URL=https://hooks.slack.com/... # For notifications
```

## Monitoring and Troubleshooting

### Log Files
- **Cron logs**: `/var/log/churnguard-etl.log`
- **Application logs**: Check your app's stdout/stderr
- **Render logs**: Available in Render Dashboard

### Common Issues

1. **Database Connection Errors**:
   - Check DATABASE_URL environment variable
   - Verify PostgreSQL server is accessible
   - Check SSL certificate configuration

2. **BigQuery Authentication Errors**:
   - Verify GOOGLE_APPLICATION_CREDENTIALS_JSON is valid JSON
   - Check BigQuery API is enabled for the project
   - Ensure service account has necessary permissions

3. **Missing Data**:
   - Check if BigQuery tables exist and have data for the target date
   - Verify account eligibility filters are working correctly
   - Check for archived accounts without archive dates

4. **Timeout Issues**:
   - Monthly rollup can take 2-3 minutes for trending risk calculations
   - Consider running monthly ETL during off-peak hours
   - Monitor memory usage for large datasets

### Health Checks

```bash
# Test database connectivity
node etl/postgresql-native/daily-metrics-etl-postgres-native.js --test-connection

# Check ETL status via API
curl https://your-app.onrender.com/api/admin/sync-status?date=2025-09-24

# Verify data integrity
psql $DATABASE_URL -c "
  SELECT
    date,
    COUNT(*) as accounts,
    SUM(total_spend) as total_spend,
    SUM(total_texts_delivered) as total_texts
  FROM daily_metrics
  WHERE date >= '2025-09-01'
  GROUP BY date
  ORDER BY date DESC
  LIMIT 7;
"
```

## Production Checklist

- [ ] Environment variables configured
- [ ] Database connectivity tested
- [ ] BigQuery authentication verified
- [ ] Cron jobs scheduled or API endpoints configured
- [ ] Log monitoring set up
- [ ] Error notification system configured (optional)
- [ ] Backup and recovery procedures documented
- [ ] Performance monitoring in place

## Recovery Procedures

### Reprocessing Historical Data

```bash
# Reprocess specific date
node etl/shared-scripts/cron-manager.js daily 2025-09-22

# Reprocess date range (requires manual loop)
for date in 2025-09-21 2025-09-22 2025-09-23; do
  node etl/shared-scripts/cron-manager.js daily $date
  sleep 60  # Wait between runs
done

# Rebuild monthly data
node etl/shared-scripts/cron-manager.js monthly 2025-09
```

### Data Validation

```sql
-- Check daily metrics coverage
SELECT
  date,
  COUNT(*) as account_count,
  SUM(CASE WHEN total_spend > 0 THEN 1 ELSE 0 END) as accounts_with_spend,
  SUM(CASE WHEN total_texts_delivered > 0 THEN 1 ELSE 0 END) as accounts_with_texts
FROM daily_metrics
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date
ORDER BY date DESC;

-- Check monthly metrics and risk levels
SELECT
  month,
  COUNT(*) as total_accounts,
  COUNT(CASE WHEN trending_risk_level = 'high' THEN 1 END) as high_risk,
  COUNT(CASE WHEN trending_risk_level = 'medium' THEN 1 END) as medium_risk,
  COUNT(CASE WHEN trending_risk_level = 'low' THEN 1 END) as low_risk
FROM monthly_metrics
WHERE month = TO_CHAR(CURRENT_DATE, 'YYYY-MM')
GROUP BY month;
```