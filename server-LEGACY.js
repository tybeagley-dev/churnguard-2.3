import express from 'express';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import path from 'path';
import { fileURLToPath } from 'url';
import cron from 'node-cron';
import { DailyProductionETL } from './scripts/daily-production-etl.js';
import { ChurnGuardCalendar } from './calendar.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = 3002;

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// Database connection
let db;

async function initDatabase() {
  db = await open({
    filename: './data/churnguard_simulation.db',
    driver: sqlite3.Database
  });
  console.log('📊 Connected to SQLite simulation database');
}

// Debug middleware
app.use((req, res, next) => {
  if (req.path.startsWith('/api')) {
    console.log(`API Request: ${req.method} ${req.path}`);
  }
  next();
});

// Response logging middleware
app.use('/api', (req, res, next) => {
  const originalSend = res.json;
  res.json = function(data) {
    console.log(`API Response ${req.method} ${req.path}:`, typeof data, Array.isArray(data) ? `Array[${data.length}]` : 'Object');
    if (data === undefined || data === null) {
      console.warn(`⚠️  NULL/UNDEFINED response for ${req.path}`);
    }
    return originalSend.call(this, data);
  };
  next();
});

// Simple test route
app.get("/api/test", (req, res) => {
  res.json({ message: "API working!", timestamp: new Date().toISOString() });
});

// Authentication endpoints - with multiple valid passwords for testing
app.post("/api/auth/login", (req, res) => {
  const { password } = req.body;
  
  // Accept both the original password and a simpler one for testing
  const validPasswords = ["Boostly123!", "Boostly123", "admin"];
  
  if (!validPasswords.includes(password)) {
    console.log('Login failed with password:', password);
    return res.status(401).json({ error: 'Invalid password' });
  }
  
  console.log('Login successful with password:', password);
  const sessionId = Math.random().toString(36).substring(7);
  res.json({ 
    success: true, 
    sessionId,
    user: { id: 'admin' }
  });
});

app.get("/api/auth/check", (req, res) => {
  // Always return authenticated for demo
  res.json({ isAuthenticated: true, user: { username: "admin" } });
});

app.post("/api/auth/logout", (req, res) => {
  res.json({ success: true });
});

app.post("/api/auth/change-password", (req, res) => {
  // Dummy endpoint for 2.1 compatibility
  res.json({ success: true });
});

// Add missing endpoints that 2.1 frontend might expect
app.get("/api/risk-scores/latest", async (req, res) => {
  // Return risk score summary for dashboard - apply same filtering as other sections
  try {
    const currentMonth = new Date().toISOString().slice(0, 7); // YYYY-MM format
    
    const riskSummary = await db.get(`
      SELECT 
        SUM(CASE WHEN a.status = 'FROZEN' 
          OR (mm.avg_active_subs_cnt < 300 AND mm.total_coupons_redeemed < 35) THEN 1 ELSE 0 END) as high_risk,
        SUM(CASE WHEN a.status != 'FROZEN' 
          AND NOT (mm.avg_active_subs_cnt < 300 AND mm.total_coupons_redeemed < 35)
          AND (mm.total_coupons_redeemed <= 3 OR mm.avg_active_subs_cnt < 300 OR mm.total_coupons_redeemed < 35) 
          THEN 1 ELSE 0 END) as medium_risk,
        COUNT(*) as total_accounts
      FROM accounts a
      INNER JOIN monthly_metrics mm ON a.account_id = mm.account_id AND mm.month = ?
      WHERE (
        -- Apply same filtering logic as Historical Performance and Monthly Trends
        DATE(a.launched_at) <= DATE(mm.month || '-01', '+1 month', '-1 day')
        AND (
          -- Account is not archived (both fields are null), OR
          (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
          -- Account was archived during or after this month started
          OR mm.month < date(COALESCE(a.archived_at, a.earliest_unit_archived_at))
        )
      )
    `, currentMonth);
    
    const low_risk = riskSummary.total_accounts - riskSummary.high_risk - riskSummary.medium_risk;
    
    res.json({
      high_risk: riskSummary.high_risk || 0,
      medium_risk: riskSummary.medium_risk || 0, 
      low_risk: low_risk || 0,
      total_accounts: riskSummary.total_accounts || 0
    });
  } catch (error) {
    console.error('Error fetching risk scores:', error);
    res.json({ high_risk: 0, medium_risk: 0, low_risk: 0, total_accounts: 0 });
  }
});

app.get("/api/test-connection", async (req, res) => {
  // Test BigQuery connection (dummy for simulation)
  res.json({ success: true, message: "SQLite simulation connection OK" });
});

// Main API endpoints that match 2.1 frontend expectations
app.get("/api/accounts", async (req, res) => {
  try {
    const currentMonth = new Date().toISOString().slice(0, 7); // YYYY-MM format
    
    // Use monthly metrics with same filtering as other sections
    const accounts = await db.all(`
      SELECT 
        a.account_id,
        a.account_name,
        a.status,
        a.csm_owner,
        a.launched_at,
        
        -- Current month totals from monthly_metrics
        COALESCE(mm.total_spend, 0) as total_spend,
        COALESCE(mm.total_texts_delivered, 0) as total_texts_delivered,
        COALESCE(mm.total_coupons_redeemed, 0) as coupons_redeemed,
        COALESCE(ROUND(mm.avg_active_subs_cnt), 0) as active_subs_cnt,
        
        -- Risk calculation using monthly data
        CASE 
          WHEN a.status = 'FROZEN' THEN 'high'
          WHEN (COALESCE(mm.avg_active_subs_cnt, 0) < 300 AND COALESCE(mm.total_coupons_redeemed, 0) < 35) THEN 'high'
          WHEN (COALESCE(mm.total_coupons_redeemed, 0) <= 3) THEN 'medium'
          WHEN (COALESCE(mm.avg_active_subs_cnt, 0) < 300 OR COALESCE(mm.total_coupons_redeemed, 0) < 35) THEN 'medium'
          ELSE 'low'
        END as risk_level
        
      FROM accounts a
      INNER JOIN monthly_metrics mm ON a.account_id = mm.account_id AND mm.month = ?
      WHERE (
        -- Apply same filtering as Historical Performance and Monthly Trends
        DATE(a.launched_at) <= DATE(mm.month || '-01', '+1 month', '-1 day')
        AND (
          -- Account is not archived (both fields are null), OR
          (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
          -- Account was archived during or after this month started
          OR mm.month < date(COALESCE(a.archived_at, a.earliest_unit_archived_at))
        )
      )
      ORDER BY 
        CASE 
          WHEN a.status = 'FROZEN' THEN 3
          WHEN (COALESCE(mm.avg_active_subs_cnt, 0) < 300 AND COALESCE(mm.total_coupons_redeemed, 0) < 35) THEN 3
          WHEN (COALESCE(mm.total_coupons_redeemed, 0) <= 3) THEN 2
          WHEN (COALESCE(mm.avg_active_subs_cnt, 0) < 300 OR COALESCE(mm.total_coupons_redeemed, 0) < 35) THEN 2
          ELSE 1
        END DESC,
        total_spend DESC
    `, currentMonth);

    res.json(accounts);
  } catch (error) {
    console.error('Error fetching accounts:', error);
    res.status(500).json({ error: 'Failed to fetch accounts data' });
  }
});

app.get("/api/bigquery/accounts", async (req, res) => {
  // Weekly View accounts endpoint - unified approach with delta calculations
  try {
    const { period = 'current_week', risk_level } = req.query;
    
    console.log(`📅 Weekly API: period=${period}`);

    // CALENDAR DATE LOGIC
    const calendarInfo = ChurnGuardCalendar.getDateInfo();
    const currentWeekStartStr = calendarInfo.week.start;
    const currentWeekEndStr = calendarInfo.week.end;

    console.log('📅 CALENDAR DATES:');
    console.log('  Week Start (Sunday):', currentWeekStartStr);
    console.log('  Week End (Most Recent Complete Day):', currentWeekEndStr);
    console.log('  Day of Week:', calendarInfo.dayOfWeek);
    
    let accounts;
    
    if (period === 'current_week') {
      // Current WTD: Raw weekly data (no deltas) - Use same account base as monthly view
      const currentMonth = ChurnGuardCalendar.getCurrentMonth();
      
      accounts = await db.all(`
        SELECT 
          a.account_id,
          a.account_name as name,
          a.status,
          a.csm_owner as csm,
          a.launched_at,
          
          -- Current week totals from daily_metrics (default to 0 if no weekly activity)
          COALESCE(wtd.total_spend, 0) as total_spend,
          COALESCE(wtd.total_texts_delivered, 0) as total_texts_delivered,
          COALESCE(wtd.total_coupons_redeemed, 0) as coupons_redeemed,
          COALESCE(ROUND(COALESCE(wtd.avg_active_subs_cnt, mm.avg_active_subs_cnt)), 0) as active_subs_cnt
          
        FROM accounts a
        INNER JOIN monthly_metrics mm ON a.account_id = mm.account_id 
          AND mm.month = ? 
          AND mm.trending_risk_level IS NOT NULL
        LEFT JOIN (
          SELECT 
            account_id,
            AVG(active_subs_cnt) as avg_active_subs_cnt,
            SUM(coupons_redeemed) as total_coupons_redeemed,
            SUM(total_spend) as total_spend,
            SUM(total_texts_delivered) as total_texts_delivered
          FROM daily_metrics 
          WHERE date >= ? AND date <= ?
          GROUP BY account_id
        ) wtd ON a.account_id = wtd.account_id
        WHERE (
          DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
          AND (
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(? || '-01')
          )
        )
      `, currentMonth, currentWeekStartStr, currentWeekEndStr, currentMonth, currentMonth);
      
      
    } else if (period === 'previous_week') {
      // Two-dataset approach: Current week (same as current_week period) + Previous week comparison

      const currentMonth = ChurnGuardCalendar.getCurrentMonth();
      const prevWeek = calendarInfo.comparisons.previousWeek;
      const prevWeekStartStr = prevWeek.start;
      const prevWeekEndStr = prevWeek.end;

      console.log('📅 Previous Week:', prevWeekStartStr, 'to', prevWeekEndStr);

      // 1. Get current week data (same as current_week period - 888 accounts)
      const currentWeekData = await db.all(`
        SELECT
          a.account_id,
          a.account_name as name,
          a.status,
          a.csm_owner as csm,
          a.launched_at,

          -- Current week totals from daily_metrics
          COALESCE(wtd.total_spend, 0) as current_total_spend,
          COALESCE(wtd.total_texts_delivered, 0) as current_total_texts_delivered,
          COALESCE(wtd.total_coupons_redeemed, 0) as current_coupons_redeemed,
          COALESCE(ROUND(COALESCE(wtd.avg_active_subs_cnt, mm.avg_active_subs_cnt)), 0) as current_active_subs_cnt

        FROM accounts a
        INNER JOIN monthly_metrics mm ON a.account_id = mm.account_id
          AND mm.month = ?
          AND mm.trending_risk_level IS NOT NULL
        LEFT JOIN (
          SELECT
            account_id,
            AVG(active_subs_cnt) as avg_active_subs_cnt,
            SUM(coupons_redeemed) as total_coupons_redeemed,
            SUM(total_spend) as total_spend,
            SUM(total_texts_delivered) as total_texts_delivered
          FROM daily_metrics
          WHERE date >= ? AND date <= ?
          GROUP BY account_id
        ) wtd ON a.account_id = wtd.account_id
        WHERE (
          DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
          AND (
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(? || '-01')
          )
        )
        ORDER BY current_total_spend DESC
      `, currentMonth, currentWeekStartStr, currentWeekEndStr, currentMonth, currentMonth);

      // 2. Get previous week comparison totals (separate optimized query)
      const comparisonTotals = await db.get(`
        SELECT
          COALESCE(SUM(dm.total_spend), 0) as total_spend,
          COALESCE(SUM(dm.total_texts_delivered), 0) as total_texts_delivered,
          COALESCE(SUM(dm.coupons_redeemed), 0) as coupons_redeemed,
          COALESCE(ROUND(AVG(dm.active_subs_cnt)), 0) as active_subs_cnt
        FROM daily_metrics dm
        INNER JOIN accounts a ON dm.account_id = a.account_id
        WHERE dm.date >= ? AND dm.date <= ?
          AND DATE(a.launched_at) <= DATE(?)
          AND (
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(?)
          )
      `, prevWeekStartStr, prevWeekEndStr, prevWeekEndStr, prevWeekStartStr);

      // 3. Add comparison data and deltas to each account
      accounts = currentWeekData.map(account => ({
        ...account,
        // Previous week totals (comparison data)
        total_spend: comparisonTotals.total_spend,
        total_texts_delivered: comparisonTotals.total_texts_delivered,
        coupons_redeemed: comparisonTotals.coupons_redeemed,
        active_subs_cnt: comparisonTotals.active_subs_cnt,
        // Delta calculations
        spend_delta: account.current_total_spend - comparisonTotals.total_spend,
        texts_delta: account.current_total_texts_delivered - comparisonTotals.total_texts_delivered,
        coupons_delta: account.current_coupons_redeemed - comparisonTotals.coupons_redeemed,
        subs_delta: account.current_active_subs_cnt - comparisonTotals.active_subs_cnt
      }));
      
    } else if (period === 'six_week_average') {
      // Two-dataset approach: Current week (same as current_week period) + Six week average comparison

      const currentMonth = ChurnGuardCalendar.getCurrentMonth();
      const sixWeekAvg = calendarInfo.comparisons.sixWeekAverage;
      const sixWeeksAgoStr = sixWeekAvg.start;
      const oneWeekAgoStr = sixWeekAvg.end;

      console.log('📅 Six Week Average:', sixWeeksAgoStr, 'to', oneWeekAgoStr);

      // 1. Get current week data (same as current_week period - 888 accounts)
      const currentWeekData = await db.all(`
        SELECT
          a.account_id,
          a.account_name as name,
          a.status,
          a.csm_owner as csm,
          a.launched_at,

          -- Current week totals from daily_metrics
          COALESCE(wtd.total_spend, 0) as current_total_spend,
          COALESCE(wtd.total_texts_delivered, 0) as current_total_texts_delivered,
          COALESCE(wtd.total_coupons_redeemed, 0) as current_coupons_redeemed,
          COALESCE(ROUND(COALESCE(wtd.avg_active_subs_cnt, mm.avg_active_subs_cnt)), 0) as current_active_subs_cnt

        FROM accounts a
        INNER JOIN monthly_metrics mm ON a.account_id = mm.account_id
          AND mm.month = ?
          AND mm.trending_risk_level IS NOT NULL
        LEFT JOIN (
          SELECT
            account_id,
            AVG(active_subs_cnt) as avg_active_subs_cnt,
            SUM(coupons_redeemed) as total_coupons_redeemed,
            SUM(total_spend) as total_spend,
            SUM(total_texts_delivered) as total_texts_delivered
          FROM daily_metrics
          WHERE date >= ? AND date <= ?
          GROUP BY account_id
        ) wtd ON a.account_id = wtd.account_id
        WHERE (
          DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
          AND (
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(? || '-01')
          )
        )
        ORDER BY current_total_spend DESC
      `, currentMonth, currentWeekStartStr, currentWeekEndStr, currentMonth, currentMonth);

      // 2. Get six week average comparison totals (separate optimized query)
      const comparisonTotals = await db.get(`
        SELECT
          COALESCE(ROUND(AVG(weekly_spend)), 0) as total_spend,
          COALESCE(ROUND(AVG(weekly_texts)), 0) as total_texts_delivered,
          COALESCE(ROUND(AVG(weekly_coupons)), 0) as coupons_redeemed,
          COALESCE(ROUND(AVG(weekly_subs)), 0) as active_subs_cnt
        FROM (
          SELECT
            strftime('%Y-%W', dm.date) as week,
            SUM(dm.total_spend) as weekly_spend,
            SUM(dm.total_texts_delivered) as weekly_texts,
            SUM(dm.coupons_redeemed) as weekly_coupons,
            AVG(dm.active_subs_cnt) as weekly_subs
          FROM daily_metrics dm
          INNER JOIN accounts a ON dm.account_id = a.account_id
          WHERE dm.date >= ? AND dm.date <= ?
            AND DATE(a.launched_at) <= DATE(?)
            AND (
              (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
              OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(?)
            )
          GROUP BY strftime('%Y-%W', dm.date)
        )
      `, sixWeeksAgoStr, oneWeekAgoStr, oneWeekAgoStr, sixWeeksAgoStr);

      // 3. Add comparison data and deltas to each account
      accounts = currentWeekData.map(account => ({
        ...account,
        // Six week average totals (comparison data)
        total_spend: comparisonTotals.total_spend,
        total_texts_delivered: comparisonTotals.total_texts_delivered,
        coupons_redeemed: comparisonTotals.coupons_redeemed,
        active_subs_cnt: comparisonTotals.active_subs_cnt,
        // Delta calculations
        spend_delta: account.current_total_spend - comparisonTotals.total_spend,
        texts_delta: account.current_total_texts_delivered - comparisonTotals.total_texts_delivered,
        coupons_delta: account.current_coupons_redeemed - comparisonTotals.coupons_redeemed,
        subs_delta: account.current_active_subs_cnt - comparisonTotals.active_subs_cnt
      }));
      
    } else if (period === 'same_week_last_year') {
      // Two-dataset approach: Current week (same as current_week period) + Same week last year comparison

      const currentMonth = ChurnGuardCalendar.getCurrentMonth();
      const lastYearWeek = calendarInfo.comparisons.sameWeekLastYear;
      const lastYearWeekStartStr = lastYearWeek.start;
      const lastYearWeekEndStr = lastYearWeek.end;

      console.log('📅 Same Week Last Year:', lastYearWeekStartStr, 'to', lastYearWeekEndStr);

      // 1. Get current week data (same as current_week period - 888 accounts)
      const currentWeekData = await db.all(`
        SELECT
          a.account_id,
          a.account_name as name,
          a.status,
          a.csm_owner as csm,
          a.launched_at,

          -- Current week totals from daily_metrics
          COALESCE(wtd.total_spend, 0) as current_total_spend,
          COALESCE(wtd.total_texts_delivered, 0) as current_total_texts_delivered,
          COALESCE(wtd.total_coupons_redeemed, 0) as current_coupons_redeemed,
          COALESCE(ROUND(COALESCE(wtd.avg_active_subs_cnt, mm.avg_active_subs_cnt)), 0) as current_active_subs_cnt

        FROM accounts a
        INNER JOIN monthly_metrics mm ON a.account_id = mm.account_id
          AND mm.month = ?
          AND mm.trending_risk_level IS NOT NULL
        LEFT JOIN (
          SELECT
            account_id,
            AVG(active_subs_cnt) as avg_active_subs_cnt,
            SUM(coupons_redeemed) as total_coupons_redeemed,
            SUM(total_spend) as total_spend,
            SUM(total_texts_delivered) as total_texts_delivered
          FROM daily_metrics
          WHERE date >= ? AND date <= ?
          GROUP BY account_id
        ) wtd ON a.account_id = wtd.account_id
        WHERE (
          DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
          AND (
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(? || '-01')
          )
        )
        ORDER BY current_total_spend DESC
      `, currentMonth, currentWeekStartStr, currentWeekEndStr, currentMonth, currentMonth);

      // 2. Get same week last year comparison totals (separate optimized query)
      const comparisonTotals = await db.get(`
        SELECT
          COALESCE(SUM(dm.total_spend), 0) as total_spend,
          COALESCE(SUM(dm.total_texts_delivered), 0) as total_texts_delivered,
          COALESCE(SUM(dm.coupons_redeemed), 0) as coupons_redeemed,
          COALESCE(ROUND(AVG(dm.active_subs_cnt)), 0) as active_subs_cnt
        FROM daily_metrics dm
        INNER JOIN accounts a ON dm.account_id = a.account_id
        WHERE dm.date >= ? AND dm.date <= ?
          AND DATE(a.launched_at) <= DATE(?)
          AND (
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(?)
          )
      `, lastYearWeekStartStr, lastYearWeekEndStr, lastYearWeekEndStr, lastYearWeekStartStr);

      // 3. Add comparison data and deltas to each account
      accounts = currentWeekData.map(account => ({
        ...account,
        // Last year same week totals (comparison data)
        total_spend: comparisonTotals.total_spend,
        total_texts_delivered: comparisonTotals.total_texts_delivered,
        coupons_redeemed: comparisonTotals.coupons_redeemed,
        active_subs_cnt: comparisonTotals.active_subs_cnt,
        // Delta calculations
        spend_delta: account.current_total_spend - comparisonTotals.total_spend,
        texts_delta: account.current_total_texts_delivered - comparisonTotals.total_texts_delivered,
        coupons_delta: account.current_coupons_redeemed - comparisonTotals.coupons_redeemed,
        subs_delta: account.current_active_subs_cnt - comparisonTotals.active_subs_cnt
      }));
      
    } else if (period === 'same_week_last_month') {
      // Two-dataset approach: Current week (same as current_week period) + Same week last month comparison

      const currentMonth = ChurnGuardCalendar.getCurrentMonth();
      const lastMonthWeek = calendarInfo.comparisons.sameWeekLastMonth;
      const lastMonthWeekStartStr = lastMonthWeek.start;
      const lastMonthWeekEndStr = lastMonthWeek.end;

      console.log('📅 Same Week Last Month:', lastMonthWeekStartStr, 'to', lastMonthWeekEndStr);

      // 1. Get current week data (same as current_week period - 888 accounts)
      const currentWeekData = await db.all(`
        SELECT
          a.account_id,
          a.account_name as name,
          a.status,
          a.csm_owner as csm,
          a.launched_at,

          -- Current week totals from daily_metrics
          COALESCE(wtd.total_spend, 0) as current_total_spend,
          COALESCE(wtd.total_texts_delivered, 0) as current_total_texts_delivered,
          COALESCE(wtd.total_coupons_redeemed, 0) as current_coupons_redeemed,
          COALESCE(ROUND(COALESCE(wtd.avg_active_subs_cnt, mm.avg_active_subs_cnt)), 0) as current_active_subs_cnt

        FROM accounts a
        INNER JOIN monthly_metrics mm ON a.account_id = mm.account_id
          AND mm.month = ?
          AND mm.trending_risk_level IS NOT NULL
        LEFT JOIN (
          SELECT
            account_id,
            AVG(active_subs_cnt) as avg_active_subs_cnt,
            SUM(coupons_redeemed) as total_coupons_redeemed,
            SUM(total_spend) as total_spend,
            SUM(total_texts_delivered) as total_texts_delivered
          FROM daily_metrics
          WHERE date >= ? AND date <= ?
          GROUP BY account_id
        ) wtd ON a.account_id = wtd.account_id
        WHERE (
          DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
          AND (
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(? || '-01')
          )
        )
        ORDER BY current_total_spend DESC
      `, currentMonth, currentWeekStartStr, currentWeekEndStr, currentMonth, currentMonth);

      // 2. Get same week last month comparison totals (separate optimized query)
      const comparisonTotals = await db.get(`
        SELECT
          COALESCE(SUM(dm.total_spend), 0) as total_spend,
          COALESCE(SUM(dm.total_texts_delivered), 0) as total_texts_delivered,
          COALESCE(SUM(dm.coupons_redeemed), 0) as coupons_redeemed,
          COALESCE(ROUND(AVG(dm.active_subs_cnt)), 0) as active_subs_cnt
        FROM daily_metrics dm
        INNER JOIN accounts a ON dm.account_id = a.account_id
        WHERE dm.date >= ? AND dm.date <= ?
          AND DATE(a.launched_at) <= DATE(?)
          AND (
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(?)
          )
      `, lastMonthWeekStartStr, lastMonthWeekEndStr, lastMonthWeekEndStr, lastMonthWeekStartStr);

      // 3. Add comparison data and deltas to each account
      accounts = currentWeekData.map(account => ({
        ...account,
        // Last month same week totals (comparison data)
        total_spend: comparisonTotals.total_spend,
        total_texts_delivered: comparisonTotals.total_texts_delivered,
        coupons_redeemed: comparisonTotals.coupons_redeemed,
        active_subs_cnt: comparisonTotals.active_subs_cnt,
        // Delta calculations
        spend_delta: account.current_total_spend - comparisonTotals.total_spend,
        texts_delta: account.current_total_texts_delivered - comparisonTotals.total_texts_delivered,
        coupons_delta: account.current_coupons_redeemed - comparisonTotals.coupons_redeemed,
        subs_delta: account.current_active_subs_cnt - comparisonTotals.active_subs_cnt
      }));
      
    } else {
      return res.status(400).json({ error: `Invalid period: ${period}` });
    }
    
    // Apply risk level calculation and filtering
    let processedAccounts = accounts.map(account => {
      // Use current week values for risk assessment (from current_* columns when available, else main columns)
      const currentSpend = account.current_total_spend !== undefined ? account.current_total_spend : account.total_spend;
      const currentTexts = account.current_total_texts_delivered !== undefined ? account.current_total_texts_delivered : account.total_texts_delivered;
      const currentCoupons = account.current_coupons_redeemed !== undefined ? account.current_coupons_redeemed : account.coupons_redeemed;
      const currentSubs = account.current_active_subs_cnt !== undefined ? account.current_active_subs_cnt : account.active_subs_cnt;
      
      let risk_level_calc = 'low';
      
      if (account.status === 'FROZEN') {
        risk_level_calc = 'high';
      } else if (currentSubs < 300 && currentCoupons < 8) {
        risk_level_calc = 'high';
      } else if (currentCoupons <= 1) {
        risk_level_calc = 'medium';
      } else if (currentSubs < 300 || currentCoupons < 8) {
        risk_level_calc = 'medium';
      }
      
      return {
        ...account,
        risk_level: risk_level_calc
      };
    });
    
    // Apply risk level filter if specified
    if (risk_level && risk_level !== 'all') {
      processedAccounts = processedAccounts.filter(account => account.risk_level === risk_level);
    }
    
    console.log(`📊 Weekly API returning ${processedAccounts.length} accounts for period: ${period}`);
    
    res.json(processedAccounts);
  } catch (error) {
    console.error('Error fetching weekly accounts:', error);
    res.status(500).json({ error: 'Failed to fetch weekly accounts data' });
  }
});

app.get("/api/bigquery/accounts/monthly", async (req, res) => {
  try {
    // Disable caching to prevent frontend infinite loops
    res.set('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.set('Pragma', 'no-cache');
    res.set('Expires', '0');

    const { period = 'current_month' } = req.query;
    const currentMonth = new Date().toISOString().slice(0, 7);

    console.log(`📅 Monthly API: period=${period}, currentMonth=${currentMonth}`);
    
    let accounts;
    
    if (period === 'current_month') {
      // Current MTD: Use monthly_metrics table with both current month trending and previous month historical data
      const prevMonth = new Date();
      prevMonth.setMonth(prevMonth.getMonth() - 1);
      const prevMonthStr = prevMonth.toISOString().slice(0, 7);
      
      accounts = await db.all(`
        SELECT 
          a.account_id,
          a.account_name as name,
          a.status,
          a.csm_owner as csm,
          a.launched_at,
          
          -- Current month totals from monthly_metrics (updated daily by ETL)
          COALESCE(cm.total_spend, 0) as total_spend,
          COALESCE(cm.total_texts_delivered, 0) as total_texts_delivered,
          COALESCE(cm.total_coupons_redeemed, 0) as coupons_redeemed,
          COALESCE(ROUND(cm.avg_active_subs_cnt), 0) as active_subs_cnt,
          1 as location_cnt,
          cm.last_updated as latest_activity,
          
          -- Current month trending risk data
          COALESCE(cm.trending_risk_level, 'low') as trending_risk_level,
          cm.trending_risk_reasons,
          
          -- Previous month historical risk data  
          COALESCE(pm.historical_risk_level, 'low') as risk_level,
          pm.risk_reasons
          
        FROM accounts a
        INNER JOIN monthly_metrics cm ON a.account_id = cm.account_id 
          AND cm.month = ? 
          AND cm.trending_risk_level IS NOT NULL
        LEFT JOIN monthly_metrics pm ON a.account_id = pm.account_id
          AND pm.month = ?
        WHERE (
          -- Apply same filtering as other endpoints
          DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
          AND (
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(? || '-01')
          )
        )
        ORDER BY 
          CASE 
            WHEN cm.trending_risk_level = 'high' THEN 3
            WHEN cm.trending_risk_level = 'medium' THEN 2
            ELSE 1
          END DESC,
          cm.total_spend DESC
      `, currentMonth, prevMonthStr, currentMonth, currentMonth);
      
      // Parse JSON risk_reasons for both historical and trending data
      accounts = accounts.map(account => ({
        ...account,
        risk_reasons: account.risk_reasons ? JSON.parse(account.risk_reasons) : ['No flags'],
        trending_risk_reasons: account.trending_risk_reasons ? JSON.parse(account.trending_risk_reasons) : ['No flags']
      }));
    } else if (period === 'previous_month') {
      // vs Previous MTD: Return unified view with all accounts from both periods
      const prevMonth = new Date();
      prevMonth.setMonth(prevMonth.getMonth() - 1);
      const prevMonthStr = prevMonth.toISOString().slice(0, 7); // YYYY-MM format
      
      const startDate = `${prevMonthStr}-01`;
      
      // Calculate same day of previous month as we are in current month
      const currentDay = new Date().getDate();
      const lastDayOfPrevMonth = new Date(prevMonth.getFullYear(), prevMonth.getMonth() + 1, 0).getDate();
      const endDay = Math.min(currentDay - 1, lastDayOfPrevMonth); // -1 to match "through previous complete day"
      const endDate = `${prevMonthStr}-${endDay.toString().padStart(2, '0')}`;
      
      console.log(`📅 Previous Month MTD: ${startDate} to ${endDate} (${endDay} days)`);
      
      // Get unified dataset with all accounts from both periods
      accounts = await db.all(`
        WITH all_relevant_accounts AS (
          -- All accounts that should be included in comparison (active in either period)
          SELECT DISTINCT a.account_id, a.account_name, a.status, a.csm_owner, a.launched_at
          FROM accounts a
          WHERE (
            -- Include if launched before end of current month (includes September launches)
            DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
            -- Include if launched before end of previous month comparison period  
            OR DATE(a.launched_at) <= DATE(?)
          )
          AND (
            -- Not archived, or archived after the start of our comparison window
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(?)
          )
        ),
        current_month_data AS (
          SELECT 
            ara.account_id,
            COALESCE(mm.total_spend, 0) as current_total_spend,
            COALESCE(mm.total_texts_delivered, 0) as current_total_texts_delivered,
            COALESCE(mm.total_coupons_redeemed, 0) as current_coupons_redeemed,
            COALESCE(ROUND(mm.avg_active_subs_cnt), 0) as current_active_subs_cnt,
            mm.last_updated as current_latest_activity,
            COALESCE(mm.trending_risk_level, 'low') as current_risk_level
          FROM all_relevant_accounts ara
          LEFT JOIN monthly_metrics mm ON ara.account_id = mm.account_id 
            AND mm.month = ? 
            AND mm.trending_risk_level IS NOT NULL
        ),
        previous_month_data AS (
          SELECT 
            ara.account_id,
            COALESCE(SUM(dm.total_spend), 0) as previous_total_spend,
            COALESCE(SUM(dm.total_texts_delivered), 0) as previous_total_texts_delivered,
            COALESCE(SUM(dm.coupons_redeemed), 0) as previous_coupons_redeemed,
            COALESCE(ROUND(AVG(dm.active_subs_cnt)), 0) as previous_active_subs_cnt,
            MAX(dm.date) as previous_latest_activity
          FROM all_relevant_accounts ara
          LEFT JOIN daily_metrics dm ON ara.account_id = dm.account_id 
            AND dm.date >= ?
            AND dm.date <= ?
          GROUP BY ara.account_id
        )
        SELECT 
          ara.account_id,
          ara.account_name as name,
          ara.status,
          ara.csm_owner as csm,
          ara.launched_at,
          
          -- Show previous month data in main columns for comparison view
          cmd.previous_total_spend as total_spend,
          cmd.previous_total_texts_delivered as total_texts_delivered,
          cmd.previous_coupons_redeemed as coupons_redeemed,
          cmd.previous_active_subs_cnt as active_subs_cnt,
          1 as location_cnt,
          COALESCE(cmd.previous_latest_activity, pmd.current_latest_activity) as latest_activity,
          
          -- Add current month data for delta calculations in frontend
          pmd.current_total_spend,
          pmd.current_total_texts_delivered,
          pmd.current_coupons_redeemed,
          pmd.current_active_subs_cnt,
          
          -- Risk calculation for comparison period
          CASE 
            WHEN ara.status = 'FROZEN' THEN 'high'
            WHEN (cmd.previous_active_subs_cnt < 300 AND cmd.previous_coupons_redeemed < 35) THEN 'high'
            WHEN (cmd.previous_coupons_redeemed <= 3) THEN 'medium'
            WHEN (cmd.previous_active_subs_cnt < 300 OR cmd.previous_coupons_redeemed < 35) THEN 'medium'
            ELSE 'low'
          END as risk_level
          
        FROM all_relevant_accounts ara
        LEFT JOIN current_month_data pmd ON ara.account_id = pmd.account_id
        LEFT JOIN previous_month_data cmd ON ara.account_id = cmd.account_id
        ORDER BY 
          (COALESCE(pmd.current_total_spend, 0) + COALESCE(cmd.previous_total_spend, 0)) DESC
      `, currentMonth, endDate, startDate, currentMonth, startDate, endDate);
    } else if (period === 'last_3_month_avg') {
      // vs Last 3 Month Average: Return unified view with all accounts from both periods
      const currentDate = new Date();
      const currentDay = currentDate.getDate();
      
      // Calculate 3 months ago, 2 months ago, and 1 month ago
      const threeMonthsAgo = new Date();
      threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);
      const twoMonthsAgo = new Date();
      twoMonthsAgo.setMonth(twoMonthsAgo.getMonth() - 2);
      const oneMonthAgo = new Date();
      oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
      
      // Get same day ranges for each of the 3 months
      const month3Start = `${threeMonthsAgo.toISOString().slice(0, 7)}-01`;
      const month3End = `${threeMonthsAgo.toISOString().slice(0, 7)}-${Math.min(currentDay - 1, new Date(threeMonthsAgo.getFullYear(), threeMonthsAgo.getMonth() + 1, 0).getDate()).toString().padStart(2, '0')}`;
      
      const month2Start = `${twoMonthsAgo.toISOString().slice(0, 7)}-01`;
      const month2End = `${twoMonthsAgo.toISOString().slice(0, 7)}-${Math.min(currentDay - 1, new Date(twoMonthsAgo.getFullYear(), twoMonthsAgo.getMonth() + 1, 0).getDate()).toString().padStart(2, '0')}`;
      
      const month1Start = `${oneMonthAgo.toISOString().slice(0, 7)}-01`;
      const month1End = `${oneMonthAgo.toISOString().slice(0, 7)}-${Math.min(currentDay - 1, new Date(oneMonthAgo.getFullYear(), oneMonthAgo.getMonth() + 1, 0).getDate()).toString().padStart(2, '0')}`;
      
      console.log(`📅 Last 3 Month Average: ${month3Start} to ${month3End}, ${month2Start} to ${month2End}, ${month1Start} to ${month1End}`);
      
      // Get unified dataset with all accounts from both periods
      accounts = await db.all(`
        WITH all_relevant_accounts AS (
          -- All accounts that should be included in comparison (active in either period)
          SELECT DISTINCT a.account_id, a.account_name, a.status, a.csm_owner, a.launched_at
          FROM accounts a
          WHERE (
            -- Include if launched before end of current month
            DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
            -- Include if launched before end of comparison periods
            OR DATE(a.launched_at) <= DATE(?)
          )
          AND (
            -- Not archived, or archived after the start of our comparison window
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(?)
          )
        ),
        current_month_data AS (
          SELECT 
            ara.account_id,
            COALESCE(mm.total_spend, 0) as current_total_spend,
            COALESCE(mm.total_texts_delivered, 0) as current_total_texts_delivered,
            COALESCE(mm.total_coupons_redeemed, 0) as current_coupons_redeemed,
            COALESCE(ROUND(mm.avg_active_subs_cnt), 0) as current_active_subs_cnt,
            mm.last_updated as current_latest_activity,
            COALESCE(mm.trending_risk_level, 'low') as current_risk_level
          FROM all_relevant_accounts ara
          LEFT JOIN monthly_metrics mm ON ara.account_id = mm.account_id 
            AND mm.month = ? 
            AND mm.trending_risk_level IS NOT NULL
        ),
        avg_comparison_data AS (
          SELECT 
            ara.account_id,
            COALESCE(ROUND(AVG(month_spend), 2), 0) as avg_total_spend,
            COALESCE(ROUND(AVG(month_texts), 0), 0) as avg_total_texts_delivered,
            COALESCE(ROUND(AVG(month_coupons), 0), 0) as avg_coupons_redeemed,
            COALESCE(ROUND(AVG(month_subs), 0), 0) as avg_active_subs_cnt,
            MAX(latest_date) as avg_latest_activity
          FROM all_relevant_accounts ara
          LEFT JOIN (
            -- Month 1 data
            SELECT 
              a.account_id,
              COALESCE(SUM(dm.total_spend), 0) as month_spend,
              COALESCE(SUM(dm.total_texts_delivered), 0) as month_texts,
              COALESCE(SUM(dm.coupons_redeemed), 0) as month_coupons,
              COALESCE(ROUND(AVG(dm.active_subs_cnt)), 0) as month_subs,
              MAX(dm.date) as latest_date,
              1 as month_num
            FROM accounts a
            LEFT JOIN daily_metrics dm ON a.account_id = dm.account_id 
              AND dm.date >= ? AND dm.date <= ?
            GROUP BY a.account_id
            
            UNION ALL
            
            -- Month 2 data
            SELECT 
              a.account_id,
              COALESCE(SUM(dm.total_spend), 0) as month_spend,
              COALESCE(SUM(dm.total_texts_delivered), 0) as month_texts,
              COALESCE(SUM(dm.coupons_redeemed), 0) as month_coupons,
              COALESCE(ROUND(AVG(dm.active_subs_cnt)), 0) as month_subs,
              MAX(dm.date) as latest_date,
              2 as month_num
            FROM accounts a
            LEFT JOIN daily_metrics dm ON a.account_id = dm.account_id 
              AND dm.date >= ? AND dm.date <= ?
            GROUP BY a.account_id
            
            UNION ALL
            
            -- Month 3 data
            SELECT 
              a.account_id,
              COALESCE(SUM(dm.total_spend), 0) as month_spend,
              COALESCE(SUM(dm.total_texts_delivered), 0) as month_texts,
              COALESCE(SUM(dm.coupons_redeemed), 0) as month_coupons,
              COALESCE(ROUND(AVG(dm.active_subs_cnt)), 0) as month_subs,
              MAX(dm.date) as latest_date,
              3 as month_num
            FROM accounts a
            LEFT JOIN daily_metrics dm ON a.account_id = dm.account_id 
              AND dm.date >= ? AND dm.date <= ?
            GROUP BY a.account_id
          ) monthly_data ON ara.account_id = monthly_data.account_id
          GROUP BY ara.account_id
        )
        SELECT 
          ara.account_id,
          ara.account_name as name,
          ara.status,
          ara.csm_owner as csm,
          ara.launched_at,
          
          -- Show average data in main columns for comparison view
          acd.avg_total_spend as total_spend,
          acd.avg_total_texts_delivered as total_texts_delivered,
          acd.avg_coupons_redeemed as coupons_redeemed,
          acd.avg_active_subs_cnt as active_subs_cnt,
          1 as location_cnt,
          COALESCE(acd.avg_latest_activity, cmd.current_latest_activity) as latest_activity,
          
          -- Add current month data for delta calculations in frontend
          cmd.current_total_spend,
          cmd.current_total_texts_delivered,
          cmd.current_coupons_redeemed,
          cmd.current_active_subs_cnt,
          
          -- Risk calculation for comparison period
          CASE 
            WHEN ara.status = 'FROZEN' THEN 'high'
            WHEN (acd.avg_active_subs_cnt < 300 AND acd.avg_coupons_redeemed < 35) THEN 'high'
            WHEN (acd.avg_coupons_redeemed <= 3) THEN 'medium'
            WHEN (acd.avg_active_subs_cnt < 300 OR acd.avg_coupons_redeemed < 35) THEN 'medium'
            ELSE 'low'
          END as risk_level
          
        FROM all_relevant_accounts ara
        LEFT JOIN current_month_data cmd ON ara.account_id = cmd.account_id
        LEFT JOIN avg_comparison_data acd ON ara.account_id = acd.account_id
        ORDER BY 
          (COALESCE(cmd.current_total_spend, 0) + COALESCE(acd.avg_total_spend, 0)) DESC
      `, currentMonth, month1End, month3Start, currentMonth, month1Start, month1End, month2Start, month2End, month3Start, month3End);
    } else if (period === 'this_month_last_year') {
      // vs This Month Last Year: Return unified view with all accounts from both periods  
      const currentDate = new Date();
      const currentDay = currentDate.getDate();
      const currentMonthStr = currentDate.toISOString().slice(0, 7); // YYYY-MM
      
      // Calculate same month last year
      const lastYear = new Date();
      lastYear.setFullYear(lastYear.getFullYear() - 1);
      const lastYearMonth = lastYear.toISOString().slice(0, 7); // YYYY-MM
      
      const lastYearStart = `${lastYearMonth}-01`;
      const lastYearEnd = `${lastYearMonth}-${Math.min(currentDay - 1, new Date(lastYear.getFullYear(), lastYear.getMonth() + 1, 0).getDate()).toString().padStart(2, '0')}`;
      
      console.log(`📅 This Month Last Year: ${lastYearStart} to ${lastYearEnd}`);
      
      // Get unified dataset with all accounts from both periods
      accounts = await db.all(`
        WITH all_relevant_accounts AS (
          -- All accounts that should be included in comparison (active in either period)
          SELECT DISTINCT a.account_id, a.account_name, a.status, a.csm_owner, a.launched_at
          FROM accounts a
          WHERE (
            -- Include if launched before end of current month
            DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
            -- Include if launched before end of last year comparison period
            OR DATE(a.launched_at) <= DATE(?)
          )
          AND (
            -- Not archived, or archived after the start of our comparison window
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(?)
          )
        ),
        current_month_data AS (
          SELECT 
            ara.account_id,
            COALESCE(mm.total_spend, 0) as current_total_spend,
            COALESCE(mm.total_texts_delivered, 0) as current_total_texts_delivered,
            COALESCE(mm.total_coupons_redeemed, 0) as current_coupons_redeemed,
            COALESCE(ROUND(mm.avg_active_subs_cnt), 0) as current_active_subs_cnt,
            mm.last_updated as current_latest_activity,
            COALESCE(mm.trending_risk_level, 'low') as current_risk_level
          FROM all_relevant_accounts ara
          LEFT JOIN monthly_metrics mm ON ara.account_id = mm.account_id 
            AND mm.month = ? 
            AND mm.trending_risk_level IS NOT NULL
        ),
        last_year_data AS (
          SELECT 
            ara.account_id,
            COALESCE(SUM(dm.total_spend), 0) as last_year_total_spend,
            COALESCE(SUM(dm.total_texts_delivered), 0) as last_year_total_texts_delivered,
            COALESCE(SUM(dm.coupons_redeemed), 0) as last_year_coupons_redeemed,
            COALESCE(ROUND(AVG(dm.active_subs_cnt)), 0) as last_year_active_subs_cnt,
            MAX(dm.date) as last_year_latest_activity
          FROM all_relevant_accounts ara
          LEFT JOIN daily_metrics dm ON ara.account_id = dm.account_id 
            AND dm.date >= ?
            AND dm.date <= ?
          GROUP BY ara.account_id
        )
        SELECT 
          ara.account_id,
          ara.account_name as name,
          ara.status,
          ara.csm_owner as csm,
          ara.launched_at,
          
          -- Show last year data in main columns for comparison view
          lyd.last_year_total_spend as total_spend,
          lyd.last_year_total_texts_delivered as total_texts_delivered,
          lyd.last_year_coupons_redeemed as coupons_redeemed,
          lyd.last_year_active_subs_cnt as active_subs_cnt,
          1 as location_cnt,
          COALESCE(lyd.last_year_latest_activity, cmd.current_latest_activity) as latest_activity,
          
          -- Add current month data for delta calculations in frontend
          cmd.current_total_spend,
          cmd.current_total_texts_delivered,
          cmd.current_coupons_redeemed,
          cmd.current_active_subs_cnt,
          
          -- Risk calculation for comparison period
          CASE 
            WHEN ara.status = 'FROZEN' THEN 'high'
            WHEN (lyd.last_year_active_subs_cnt < 300 AND lyd.last_year_coupons_redeemed < 35) THEN 'high'
            WHEN (lyd.last_year_coupons_redeemed <= 3) THEN 'medium'
            WHEN (lyd.last_year_active_subs_cnt < 300 OR lyd.last_year_coupons_redeemed < 35) THEN 'medium'
            ELSE 'low'
          END as risk_level
          
        FROM all_relevant_accounts ara
        LEFT JOIN current_month_data cmd ON ara.account_id = cmd.account_id
        LEFT JOIN last_year_data lyd ON ara.account_id = lyd.account_id
        ORDER BY 
          (COALESCE(cmd.current_total_spend, 0) + COALESCE(lyd.last_year_total_spend, 0)) DESC
      `, currentMonthStr, lastYearEnd, lastYearStart, currentMonthStr, lastYearStart, lastYearEnd);
    } else {
      // Other periods not yet implemented
      console.log(`⚠️  Unsupported period: ${period}`);
      accounts = [];
    }
    
    // Sort accounts by risk level and total spend (applies to both current and historical)
    accounts.sort((a, b) => {
      const riskOrder = { 'high': 3, 'medium': 2, 'low': 1 };
      if (riskOrder[a.risk_level] !== riskOrder[b.risk_level]) {
        return riskOrder[b.risk_level] - riskOrder[a.risk_level];
      }
      return b.total_spend - a.total_spend;
    });

    res.json(accounts);
  } catch (error) {
    console.error('Error fetching monthly accounts:', error);
    res.status(500).json({ error: 'Failed to fetch monthly accounts data' });
  }
});

app.get("/api/bigquery/account-history/:accountId", async (req, res) => {
  try {
    const { accountId } = req.params;
    
    const history = await db.all(`
      SELECT 
        date,
        total_spend,
        total_texts_delivered,
        coupons_redeemed,
        active_subs_cnt
      FROM daily_metrics 
      WHERE account_id = ?
      ORDER BY date DESC
      LIMIT 84  -- ~12 weeks
    `, accountId);

    res.json(history);
  } catch (error) {
    console.error('Error fetching account history:', error);
    res.status(500).json({ error: 'Failed to fetch account history' });
  }
});

app.get("/api/historical-performance", async (_req, res) => {
  try {
    // Force no cache
    res.set({
      'Cache-Control': 'no-cache, no-store, must-revalidate', 
      'Pragma': 'no-cache',
      'Expires': '0'
    });
    // Include archived accounts through the month they were archived, exclude only after
    // Exclude current month from historical view
    const historical = await db.all(`
      SELECT 
        mm.month as period,
        mm.month_label as periodLabel,
        COUNT(DISTINCT mm.account_id) as total_accounts,
        ROUND(SUM(mm.total_spend), 2) as spend_adjusted,
        SUM(mm.total_texts_delivered) as total_texts_sent,
        SUM(mm.total_coupons_redeemed) as total_redemptions,
        SUM(mm.avg_active_subs_cnt) as total_subscribers
      FROM monthly_metrics mm
      INNER JOIN accounts a ON mm.account_id = a.account_id
      WHERE (
        -- Correct eligibility: launched by month-end, not archived before month start
        a.launched_at IS NOT NULL
        AND a.launched_at < datetime(date(mm.month || '-01'), '+1 month')
        AND (
          COALESCE(a.archived_at, a.earliest_unit_archived_at) IS NULL
          OR COALESCE(a.archived_at, a.earliest_unit_archived_at) >= date(mm.month || '-01')
        )
      )
      AND mm.month >= strftime('%Y-%m', 'now', '-12 months')
      AND mm.month < strftime('%Y-%m', 'now')
      GROUP BY mm.month, mm.month_label
      ORDER BY mm.month ASC
    `);

    // Debug: Log August 2025 count
    const augustData = historical.find(row => row.period === '2025-08');
    if (augustData) {
      console.log(`📊 Historical Performance - August 2025: ${augustData.total_accounts} accounts`);
    }

    // Format to match 2.1 structure
    const formatted = historical.map(row => ({
      period: row.period,
      periodLabel: row.periodLabel,
      month_label: row.periodLabel,
      total_accounts: row.total_accounts,
      spend_adjusted: row.spend_adjusted,
      total_texts_sent: row.total_texts_sent,
      total_redemptions: row.total_redemptions,
      total_subscribers: row.total_subscribers
    }));

    res.json(formatted);
  } catch (error) {
    console.error('Error fetching historical performance:', error);
    res.status(500).json({ error: 'Failed to fetch historical performance data' });
  }
});

app.get("/api/monthly-trends", async (_req, res) => {
  try {
    // Force no cache
    res.set({
      'Cache-Control': 'no-cache, no-store, must-revalidate',
      'Pragma': 'no-cache',
      'Expires': '0'
    });
    console.log('📊 Fetching Monthly Trends data...');
    
    // Get current month for visual distinction
    const currentMonth = new Date().toISOString().slice(0, 7); // YYYY-MM format
    
    // Get 13 months: 12 prior months + current month
    const trends = await db.all(`
      SELECT 
        mm.month,
        COUNT(DISTINCT mm.account_id) as total_accounts,
        
        -- Use pre-calculated historical risk levels
        SUM(CASE WHEN COALESCE(mm.trending_risk_level, mm.historical_risk_level) = 'high' THEN 1 ELSE 0 END) as high_risk,
        SUM(CASE WHEN COALESCE(mm.trending_risk_level, mm.historical_risk_level) = 'medium' THEN 1 ELSE 0 END) as medium_risk,
        SUM(CASE WHEN COALESCE(mm.trending_risk_level, mm.historical_risk_level) = 'low' THEN 1 ELSE 0 END) as low_risk
        
      FROM monthly_metrics mm
      INNER JOIN accounts a ON mm.account_id = a.account_id
      WHERE (
        -- Correct eligibility: launched by month-end, not archived before month start
        a.launched_at IS NOT NULL
        AND a.launched_at < datetime(date(mm.month || '-01'), '+1 month')
        AND (
          COALESCE(a.archived_at, a.earliest_unit_archived_at) IS NULL
          OR COALESCE(a.archived_at, a.earliest_unit_archived_at) >= date(mm.month || '-01')
        )
      )
      AND mm.month >= strftime('%Y-%m', 'now', '-12 months')
      AND mm.month <= strftime('%Y-%m', 'now')
      GROUP BY mm.month
      ORDER BY mm.month ASC
    `);

    // Helper function to create abbreviated month labels
    const createAbbreviatedLabel = (monthStr) => {
      const [year, month] = monthStr.split('-');
      const monthIndex = parseInt(month, 10) - 1;
      const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                     'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      return `${months[monthIndex]} ${year}`;
    };

    // Transform data to include labels and current month indicator
    const transformedTrends = trends.map(row => ({
      ...row,
      month_label: createAbbreviatedLabel(row.month),
      is_current_month: row.month === currentMonth
    }));

    console.log(`✅ Monthly Trends data: ${transformedTrends.length} months`);
    console.log(`🔍 Current month: ${currentMonth}`);
    
    // Debug: Log August 2025 count
    const augustData = transformedTrends.find(row => row.month === '2025-08');
    if (augustData) {
      console.log(`📊 Monthly Trends - August 2025: ${augustData.total_accounts} accounts`);
    }
    
    res.json(transformedTrends);
  } catch (error) {
    console.error('Error fetching monthly trends:', error);
    res.status(500).json({ error: 'Failed to fetch monthly trends data' });
  }
});

// Redirect old BigQuery endpoints to SQLite endpoints for backward compatibility
app.get("/api/bigquery/monthly-trends", async (req, res) => {
  // Redirect to the SQLite-based monthly trends endpoint
  req.url = '/api/monthly-trends';
  return app._router.handle(req, res);
});

// Claude 12-month historical performance data from SQLite
app.get("/api/bigquery/claude-12month", async (_req, res) => {
  try {
    // Force no cache
    res.set({
      'Cache-Control': 'no-cache, no-store, must-revalidate',
      'Pragma': 'no-cache',
      'Expires': '0'
    });
    console.log('📊 Fetching claude 12-month historical performance data...');
    
    // Get historical data for 12 completed months, excluding current month
    // Rolling 12 months: always shows 12 months ending with last completed month
    const historicalData = await db.all(`
      SELECT 
        mm.month,
        mm.month_label,
        COUNT(DISTINCT mm.account_id) as total_accounts,
        SUM(mm.total_spend) as spend_adjusted,
        SUM(mm.total_coupons_redeemed) as total_redemptions,
        SUM(mm.avg_active_subs_cnt) as total_subscribers,
        SUM(mm.total_texts_delivered) as total_texts_sent
      FROM monthly_metrics mm
      INNER JOIN accounts a ON mm.account_id = a.account_id
      WHERE mm.month >= strftime('%Y-%m', 'now', '-12 months')
      AND mm.month < strftime('%Y-%m', 'now')
      AND (
        -- Correct eligibility: launched by month-end, not archived before month start
        a.launched_at IS NOT NULL
        AND a.launched_at < datetime(date(mm.month || '-01'), '+1 month')
        AND (
          COALESCE(a.archived_at, a.earliest_unit_archived_at) IS NULL
          OR COALESCE(a.archived_at, a.earliest_unit_archived_at) >= date(mm.month || '-01')
        )
      )
      GROUP BY mm.month, mm.month_label
      ORDER BY mm.month ASC
    `);

    // Helper function to create abbreviated month labels
    const createAbbreviatedLabel = (monthStr) => {
      // Fix timezone issue by parsing manually instead of using Date constructor
      const [year, month] = monthStr.split('-');
      const monthIndex = parseInt(month, 10) - 1; // Convert to 0-based index
      const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                     'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      
      return `${months[monthIndex]} ${year}`;
    };

    // Debug: Log raw data to see what we're getting
    console.log('📊 Claude 12-month raw data sample:', historicalData.slice(-2));
    
    // Transform data to match Claude12MonthData interface
    const transformedData = historicalData.map((row, index) => {
      const previousRow = index > 0 ? historicalData[index - 1] : null;
      
      // Calculate percentage changes
      const spend_change_pct = previousRow 
        ? ((row.spend_adjusted - previousRow.spend_adjusted) / previousRow.spend_adjusted) * 100
        : null;
      
      const accounts_change_pct = previousRow
        ? ((row.total_accounts - previousRow.total_accounts) / previousRow.total_accounts) * 100
        : null;


      const transformed = {
        month: row.month, // Keep original month field
        month_label: createAbbreviatedLabel(row.month),
        month_yr: row.month,
        total_accounts: row.total_accounts || 0,
        accounts_below_minimum: 0, // Not available in our SQLite schema
        spend_original: row.spend_adjusted || 0,
        spend_adjusted: row.spend_adjusted || 0,
        spend_adjustment: 0, // Not available in our SQLite schema
        total_redemptions: row.total_redemptions || 0,
        total_subscribers: Math.round(row.total_subscribers || 0),
        total_texts_sent: row.total_texts_sent || 0,
        spend_change_pct: spend_change_pct ? Math.round(spend_change_pct * 100) / 100 : null,
        accounts_change_pct: accounts_change_pct ? Math.round(accounts_change_pct * 100) / 100 : null,
        total_spend_12mo: 0, // Would need rolling calculation
        total_adjustment_12mo: 0, // Not available
        total_redemptions_12mo: 0, // Would need rolling calculation
        total_texts_12mo: 0, // Would need rolling calculation
      };


      return transformed;
    });

    console.log(`✅ Claude 12-month data: ${transformedData.length} months`);
    
    // Debug the exact issue: check if raw data contains duplicate or wrong values
    console.log('🔍 RAW historicalData - July/August check:');
    historicalData.forEach((row, idx) => {
      if (row.month === '2025-07' || row.month === '2025-08') {
        console.log(`Index ${idx}: ${row.month} -> ${row.total_accounts} accounts, $${row.spend_adjusted}, ${row.total_texts_sent} texts`);
      }
    });
    
    const augustRaw = historicalData.find(d => d.month === '2025-08');
    const julyRaw = historicalData.find(d => d.month === '2025-07');
    if (augustRaw) {
      console.log('🔍 August 2025 RAW SQL Data:', {
        month: augustRaw.month,
        accounts: augustRaw.total_accounts,
        spend: augustRaw.spend_adjusted,
        texts: augustRaw.total_texts_sent,
        redemptions: augustRaw.total_redemptions,
        subscribers: augustRaw.total_subscribers
      });
    }
    if (julyRaw) {
      console.log('🔍 July 2025 RAW SQL Data:', {
        month: julyRaw.month,
        accounts: julyRaw.total_accounts,
        spend: julyRaw.spend_adjusted,
        texts: julyRaw.total_texts_sent,
        redemptions: julyRaw.total_redemptions,
        subscribers: julyRaw.total_subscribers
      });
    }

    // Debug log August data to investigate the drop
    const augustData = transformedData.find(d => d.month_label.includes('Aug 2025'));
    const julyData = transformedData.find(d => d.month_label.includes('Jul 2025'));
    if (augustData) {
      console.log('🔍 August 2025 TRANSFORMED Data:', {
        month: augustData.month_label,
        accounts: augustData.total_accounts,
        spend: augustData.spend_adjusted,
        texts: augustData.total_texts_sent,
        redemptions: augustData.total_redemptions,
        subscribers: augustData.total_subscribers
      });
    }
    if (julyData) {
      console.log('🔍 July 2025 TRANSFORMED Data:', {
        month: julyData.month_label,
        accounts: julyData.total_accounts,
        spend: julyData.spend_adjusted,
        texts: julyData.total_texts_sent,
        redemptions: julyData.total_redemptions,
        subscribers: julyData.total_subscribers
      });
    }
    res.json(transformedData);
  } catch (error) {
    console.error('❌ Error fetching claude 12-month data:', error);
    res.status(500).json({ error: 'Failed to fetch historical performance data' });
  }
});

// Dashboard analytics endpoint using SQLite data
app.get("/api/analytics/dashboard", async (_req, res) => {
  try {
    console.log('📊 Fetching dashboard analytics...');
    
    // Get current risk level counts
    const riskCounts = await db.get(`
      SELECT 
        COUNT(*) as totalAccounts,
        SUM(CASE WHEN a.risk_level = 'high' THEN 1 ELSE 0 END) as highRiskCount,
        SUM(CASE WHEN a.risk_level = 'medium' THEN 1 ELSE 0 END) as mediumRiskCount,
        SUM(CASE WHEN a.risk_level = 'low' THEN 1 ELSE 0 END) as lowRiskCount
      FROM accounts a
      WHERE a.status IN ('LAUNCHED', 'FROZEN')
    `);

    // Get total revenue from current month
    const revenueData = await db.get(`
      SELECT 
        SUM(a.total_spend) as totalRevenue,
        SUM(CASE WHEN a.risk_level = 'high' THEN a.total_spend ELSE 0 END) as revenueAtRisk
      FROM accounts a
      WHERE a.status IN ('LAUNCHED', 'FROZEN')
    `);

    // Get weekly trends (simplified - using daily_metrics if available)
    const weeklyTrends = await db.all(`
      SELECT 
        strftime('%Y-W%W', dm.date) as week,
        SUM(dm.total_spend) as totalSpend,
        SUM(dm.total_texts_delivered) as totalTexts,
        COUNT(DISTINCT dm.account_id) as activeAccounts,
        0 as churnedAccounts
      FROM daily_metrics dm
      INNER JOIN accounts a ON dm.account_id = a.account_id
      WHERE dm.date >= date('now', '-8 weeks')
      AND a.status IN ('LAUNCHED', 'FROZEN')
      GROUP BY strftime('%Y-W%W', dm.date)
      ORDER BY week DESC
      LIMIT 8
    `);

    const dashboardData = {
      totalAccounts: riskCounts?.totalAccounts || 0,
      highRiskCount: riskCounts?.highRiskCount || 0,
      mediumRiskCount: riskCounts?.mediumRiskCount || 0,
      lowRiskCount: riskCounts?.lowRiskCount || 0,
      totalRevenue: revenueData?.totalRevenue || 0,
      revenueAtRisk: revenueData?.revenueAtRisk || 0,
      weeklyTrends: weeklyTrends.map(row => ({
        week: row.week,
        totalSpend: row.totalSpend || 0,
        totalTexts: row.totalTexts || 0,
        activeAccounts: row.activeAccounts || 0,
        churnedAccounts: row.churnedAccounts || 0
      }))
    };

    console.log(`✅ Dashboard analytics: ${dashboardData.totalAccounts} accounts, $${dashboardData.totalRevenue} revenue`);
    res.json(dashboardData);
  } catch (error) {
    console.error('❌ Error fetching dashboard analytics:', error);
    res.status(500).json({ error: 'Failed to fetch dashboard analytics' });
  }
});

// Debug endpoint to check all data
app.get("/api/debug", async (_req, res) => {
  try {
    const [accounts, riskScores, historical, trends] = await Promise.all([
      db.all("SELECT * FROM accounts LIMIT 1"),
      db.get(`
        SELECT 
          SUM(CASE WHEN a.status = 'FROZEN' 
            OR (mm.avg_active_subs_cnt < 300 AND mm.total_coupons_redeemed < 35) THEN 1 ELSE 0 END) as high_risk,
          SUM(CASE WHEN a.status != 'FROZEN' 
            AND NOT (mm.avg_active_subs_cnt < 300 AND mm.total_coupons_redeemed < 35)
            AND (mm.total_coupons_redeemed <= 3 OR mm.avg_active_subs_cnt < 300 OR mm.total_coupons_redeemed < 35) 
            THEN 1 ELSE 0 END) as medium_risk,
          COUNT(*) as total_accounts
        FROM accounts a
        LEFT JOIN monthly_metrics mm ON a.account_id = mm.account_id AND mm.month = '2025-08'
      `),
      db.all("SELECT * FROM monthly_metrics LIMIT 1"),
      db.all("SELECT * FROM daily_metrics LIMIT 1")
    ]);

    res.json({
      message: "Debug info",
      samples: {
        account: accounts[0],
        riskScore: riskScores,
        monthlyMetric: historical[0],
        dailyMetric: trends[0]
      },
      counts: {
        accounts: await db.get("SELECT COUNT(*) as count FROM accounts"),
        dailyMetrics: await db.get("SELECT COUNT(*) as count FROM daily_metrics"),
        monthlyMetrics: await db.get("SELECT COUNT(*) as count FROM monthly_metrics")
      }
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Health check
app.get("/health", (_req, res) => {
  res.json({ status: "ok", database: "SQLite simulation" });
});

// Serve static files AFTER API routes
app.use(express.static('dist'));

// Catch-all handler: send back React's index.html file for client-side routing
app.get("*", (_req, res) => {
  res.sendFile(path.join(__dirname, "dist", "index.html"));
});

// Daily ETL Scheduler Setup
function setupDailyETLScheduler() {
  const etl = new DailyProductionETL();
  
  console.log('⏰ Setting up daily ETL scheduler...');
  
  // Run at 6:00 AM UTC every day (2 AM EST, 11 PM PST previous day)
  // This ensures fresh data is ready when users log in each morning
  cron.schedule('0 6 * * *', async () => {
    console.log('\n🌅 Daily ETL cron job triggered');
    try {
      const result = await etl.runDailyPipeline();
      console.log('✅ Automated daily ETL completed successfully:', {
        date: result.processDate,
        duration: result.duration,
        accounts: result.extractResults.totalAccounts
      });
    } catch (error) {
      console.error('❌ Automated daily ETL failed:', error);
      // In production, this could send alerts to Slack/email
    }
  }, {
    scheduled: true,
    timezone: "UTC"
  });
  
  console.log('✅ Daily ETL scheduler active - runs at 6:00 AM UTC daily');
  console.log('🔧 Manual trigger available at: POST /api/trigger-etl');
}

// Manual ETL trigger endpoint for testing/emergency use
app.post('/api/trigger-etl', async (req, res) => {
  const { date } = req.body;
  
  console.log(`🚀 Manual ETL trigger requested for ${date || 'yesterday'}`);
  
  try {
    const etl = new DailyProductionETL();
    const result = await etl.runDailyPipeline(date);
    
    res.json({
      success: true,
      message: 'Manual ETL completed successfully',
      result
    });
  } catch (error) {
    console.error('❌ Manual ETL failed:', error);
    res.status(500).json({
      success: false,
      message: 'Manual ETL failed',
      error: error.message
    });
  }
});

// Start server
async function start() {
  try {
    await initDatabase();
    
    app.listen(PORT, () => {
      console.log(`🚀 ChurnGuard 2.3 (Production ETL Ready) running at http://localhost:${PORT}`);
      console.log(`📊 Serving data from SQLite simulation database`);
      console.log(`🎯 Ready to test production-ready ETL pipeline!`);
      console.log(`🔄 Data: ${new Date().toISOString()}`);
      
      // Initialize daily ETL cron job
      setupDailyETLScheduler();
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

start();