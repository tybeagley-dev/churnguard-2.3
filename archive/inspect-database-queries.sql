-- ChurnGuard 2.2 Simulation Database Inspection Queries

-- 1. Database Overview
SELECT 'Accounts' as table_name, COUNT(*) as records FROM accounts
UNION ALL  
SELECT 'Daily Metrics' as table_name, COUNT(*) as records FROM daily_metrics;

-- 2. Account Status Distribution
SELECT status, COUNT(*) as count 
FROM accounts 
GROUP BY status 
ORDER BY count DESC;

-- 3. Date Range Coverage
SELECT 
  MIN(date) as earliest_date,
  MAX(date) as latest_date,
  COUNT(DISTINCT date) as days_covered
FROM daily_metrics;

-- 4. Daily Activity Summary (last 10 days)
SELECT 
  date,
  COUNT(*) as accounts_with_activity,
  SUM(total_spend) as total_spend,
  SUM(total_texts_delivered) as total_texts,
  SUM(coupons_redeemed) as total_coupons,
  AVG(active_subs_cnt) as avg_subscribers
FROM daily_metrics 
WHERE date >= (SELECT MAX(date) FROM daily_metrics) - INTERVAL 10 DAY
GROUP BY date 
ORDER BY date DESC;

-- 5. Top 10 Accounts by Spend (September 2025)
SELECT 
  a.account_name,
  a.status,
  SUM(dm.total_spend) as september_spend,
  SUM(dm.total_texts_delivered) as september_texts,
  SUM(dm.coupons_redeemed) as september_coupons
FROM accounts a
JOIN daily_metrics dm ON a.account_id = dm.account_id
WHERE dm.date >= '2025-09-01' AND dm.date <= '2025-09-03'
GROUP BY a.account_id, a.account_name, a.status
ORDER BY september_spend DESC
LIMIT 10;

-- 6. Accounts with No Activity (Potential Data Quality Issues)
SELECT 
  a.account_name,
  a.status,
  a.launched_at,
  COUNT(dm.date) as days_with_data
FROM accounts a
LEFT JOIN daily_metrics dm ON a.account_id = dm.account_id
GROUP BY a.account_id, a.account_name, a.status, a.launched_at
HAVING days_with_data = 0
ORDER BY a.account_name;

-- 7. Monthly Aggregation (September 2025 so far)
SELECT 
  '2025-09' as month,
  COUNT(DISTINCT dm.account_id) as active_accounts,
  SUM(dm.total_spend) as total_spend,
  SUM(dm.total_texts_delivered) as total_texts,
  SUM(dm.coupons_redeemed) as total_coupons,
  AVG(dm.active_subs_cnt) as avg_subscribers_per_account
FROM daily_metrics dm
WHERE dm.date >= '2025-09-01' AND dm.date <= '2025-09-03'
GROUP BY '2025-09';

-- 8. Data Quality Check - Records by ETL Component
SELECT 
  date,
  COUNT(CASE WHEN total_spend > 0 THEN 1 END) as accounts_with_spend,
  COUNT(CASE WHEN total_texts_delivered > 0 THEN 1 END) as accounts_with_texts,
  COUNT(CASE WHEN coupons_redeemed > 0 THEN 1 END) as accounts_with_coupons,
  COUNT(CASE WHEN active_subs_cnt > 0 THEN 1 END) as accounts_with_subscribers
FROM daily_metrics
WHERE date >= '2025-09-01'
GROUP BY date
ORDER BY date;