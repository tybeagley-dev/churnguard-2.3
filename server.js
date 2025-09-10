import express from 'express';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import path from 'path';
import { fileURLToPath } from 'url';
import cron from 'node-cron';
import { DailyProductionETL } from './scripts/daily-production-etl.js';

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
  // Weekly View accounts endpoint - show raw week-to-date data
  try {
    const currentDate = new Date();
    const { risk_level } = req.query;
    
    console.log('Weekly View - Risk level filter requested:', risk_level);
    
    // Calculate current week dates (Monday to today)
    const today = new Date(currentDate);
    const dayOfWeek = today.getDay(); // 0 = Sunday, 1 = Monday, ..., 6 = Saturday
    const mondayOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek; // Get Monday of current week
    
    const monday = new Date(today);
    monday.setDate(today.getDate() + mondayOffset);
    monday.setHours(0, 0, 0, 0);
    
    const startOfWeek = monday.toISOString().split('T')[0]; // YYYY-MM-DD format
    const currentDateStr = today.toISOString().split('T')[0]; // YYYY-MM-DD format
    
    console.log(`Weekly View: ${startOfWeek} to ${currentDateStr}`);
    
    // Get raw WTD data from daily_metrics (current week only)
    const rawAccounts = await db.all(`
      SELECT 
        a.account_id,
        a.account_name,
        a.status,
        a.csm_owner,
        a.launched_at,
        
        -- Current week totals from daily_metrics (WTD) - NO proportional calculations
        COALESCE(wtd.total_spend, 0) as total_spend,
        COALESCE(wtd.total_texts_delivered, 0) as total_texts_delivered,
        COALESCE(wtd.total_coupons_redeemed, 0) as coupons_redeemed,
        COALESCE(ROUND(wtd.avg_active_subs_cnt), 0) as active_subs_cnt
        
      FROM accounts a
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
        -- Apply same filtering as Monthly View
        DATE(a.launched_at) <= DATE(?)
        AND (
          (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
          OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(?)
        )
      )
    `, startOfWeek, currentDateStr, currentDateStr, startOfWeek);
    
    // Apply raw risk level calculation (no projections for weekly data)
    let accounts = rawAccounts.map(account => {
      // Use raw WTD values for risk assessment
      let risk_level_calc = 'low';
      
      if (account.status === 'FROZEN') {
        risk_level_calc = 'high';
      } else if (account.active_subs_cnt < 300 && account.coupons_redeemed < 8) { // ~8 coupons per week vs 35/month
        risk_level_calc = 'high';
      } else if (account.coupons_redeemed <= 1) {
        risk_level_calc = 'medium';
      } else if (account.active_subs_cnt < 300 || account.coupons_redeemed < 8) {
        risk_level_calc = 'medium';
      }
      
      return {
        ...account,
        risk_level: risk_level_calc
      };
    });
    
    // Apply risk level filter if specified
    if (risk_level && risk_level !== 'all') {
      accounts = accounts.filter(account => account.risk_level === risk_level);
    }
    
    // Sort by risk level priority then by spend
    accounts.sort((a, b) => {
      const riskOrder = { 'high': 3, 'medium': 2, 'low': 1 };
      if (riskOrder[a.risk_level] !== riskOrder[b.risk_level]) {
        return riskOrder[b.risk_level] - riskOrder[a.risk_level];
      }
      return b.total_spend - a.total_spend;
    });

    res.json(accounts);
  } catch (error) {
    console.error('Error fetching weekly accounts:', error);
    res.status(500).json({ error: 'Failed to fetch weekly accounts data' });
  }
});

app.get("/api/bigquery/accounts/monthly", async (req, res) => {
  try {
    const { month = new Date().toISOString().slice(0, 7) } = req.query; // YYYY-MM format
    const currentMonth = new Date().toISOString().slice(0, 7);
    const isCurrentMonth = month === currentMonth;
    
    console.log(`📅 Monthly API: month=${month}, currentMonth=${currentMonth}, isCurrentMonth=${isCurrentMonth}`);
    
    let accounts;
    
    if (isCurrentMonth) {
      // Current month: Use daily_metrics with proportional trending analysis
      const currentDate = new Date();
      const currentDay = currentDate.getDate();
      const daysInMonth = new Date(currentDate.getFullYear(), currentDate.getMonth() + 1, 0).getDate();
      const progressPercentage = Math.max((currentDay - 1) / daysInMonth, 0.01); // Avoid division by zero
      
      // First get the raw MTD data
      const rawAccounts = await db.all(`
        SELECT 
          a.account_id,
          a.account_name,
          a.status,
          a.csm_owner,
          a.launched_at,
          
          -- Current month totals from daily_metrics
          COALESCE(SUM(dm.total_spend), 0) as total_spend,
          COALESCE(SUM(dm.total_texts_delivered), 0) as total_texts_delivered,
          COALESCE(SUM(dm.coupons_redeemed), 0) as coupons_redeemed,
          COALESCE(ROUND(AVG(dm.active_subs_cnt)), 0) as active_subs_cnt
          
        FROM accounts a
        LEFT JOIN daily_metrics dm ON a.account_id = dm.account_id AND dm.date >= ? || '-01' AND dm.date <= DATE('now')
        WHERE (
          -- Apply same filtering as Monthly View
          DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
          AND (
            -- Account is not archived (both fields are null), OR
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            -- Account was archived during or after this month started
            OR mm.month < date(COALESCE(a.archived_at, a.earliest_unit_archived_at))
          )
        )
        GROUP BY a.account_id, a.account_name, a.status, a.csm_owner, a.launched_at
      `, month, month, month);
      
      // Apply proportional trending analysis in JavaScript
      console.log(`🔄 Applying trending analysis: Day ${currentDay}/${daysInMonth}, Progress: ${(progressPercentage * 100).toFixed(1)}%`);
      
      accounts = rawAccounts.map(account => {
        // Calculate projected month-end values
        const projectedCoupons = account.coupons_redeemed / progressPercentage;
        
        // Determine risk level using projections
        let risk_level = 'low';
        
        if (account.status === 'FROZEN') {
          risk_level = 'high';
        } else if (account.active_subs_cnt < 300 && projectedCoupons < 35) {
          risk_level = 'high';
        } else if (projectedCoupons <= 3) {
          risk_level = 'medium';
        } else if (account.active_subs_cnt < 300 || projectedCoupons < 35) {
          risk_level = 'medium';
        }
        
        return {
          ...account,
          risk_level
        };
      });
    } else {
      // Historical months: Use monthly_metrics table
      accounts = await db.all(`
        SELECT 
          a.account_id,
          a.account_name,
          a.status,
          a.csm_owner,
          a.launched_at,
          
          -- Monthly totals from monthly_metrics table
          COALESCE(mm.total_spend, 0) as total_spend,
          COALESCE(mm.total_texts_delivered, 0) as total_texts_delivered,
          COALESCE(mm.total_coupons_redeemed, 0) as coupons_redeemed,
          COALESCE(ROUND(mm.avg_active_subs_cnt), 0) as active_subs_cnt,
          
          -- Risk calculation using historical data
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
          -- Use same eligibility logic as ETL pipeline
          a.status IN ('LAUNCHED', 'FROZEN') OR 
          (a.status = 'ARCHIVED' AND 
           mm.month < date(COALESCE(a.archived_at, a.earliest_unit_archived_at)))
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
      `, month);
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
        -- Use same eligibility logic as ETL pipeline
        a.status IN ('LAUNCHED', 'FROZEN') OR 
        (a.status = 'ARCHIVED' AND 
         mm.month < date(COALESCE(a.archived_at, a.earliest_unit_archived_at)))
      )
      AND mm.month >= strftime('%Y-%m', 'now', '-12 months')
      AND mm.month < strftime('%Y-%m', 'now')
      GROUP BY mm.month, mm.month_label
      ORDER BY mm.month ASC
    `);

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
    // Use pre-calculated historical_risk_level for accurate risk distribution
    const trends = await db.all(`
      SELECT 
        mm.month,
        COUNT(*) as total_accounts,
        
        -- Use pre-calculated historical risk levels
        SUM(CASE WHEN mm.historical_risk_level = 'high' THEN 1 ELSE 0 END) as high_risk,
        SUM(CASE WHEN mm.historical_risk_level = 'medium' THEN 1 ELSE 0 END) as medium_risk,
        SUM(CASE WHEN mm.historical_risk_level = 'low' THEN 1 ELSE 0 END) as low_risk
        
      FROM monthly_metrics mm
      INNER JOIN accounts a ON mm.account_id = a.account_id
      WHERE (
        -- Use same eligibility logic as ETL pipeline (same as Historical Performance)
        a.status IN ('LAUNCHED', 'FROZEN') OR 
        (a.status = 'ARCHIVED' AND 
         mm.month < date(COALESCE(a.archived_at, a.earliest_unit_archived_at)))
      )
      AND mm.month >= strftime('%Y-%m', 'now', '-11 months')
      AND mm.month <= strftime('%Y-%m', 'now')
      GROUP BY mm.month
      ORDER BY mm.month ASC
    `);

    res.json(trends);
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
      WHERE mm.month >= date('now', '-13 months', 'start of month')
      AND mm.month < date('now', '-1 month', 'start of month')
      AND (
        a.status IN ('LAUNCHED', 'FROZEN') OR 
        (a.status = 'ARCHIVED' AND 
         mm.month < date(COALESCE(a.archived_at, a.earliest_unit_archived_at)))
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
app.use(express.static('public'));

// Catch-all handler: send back React's index.html file for client-side routing
app.get("*", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
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