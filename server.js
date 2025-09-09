import express from 'express';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import path from 'path';
import { fileURLToPath } from 'url';

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
          OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(mm.month || '-01')
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
          OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(mm.month || '-01')
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
    // Monthly view - use monthly_metrics table for much faster queries
    const { month = new Date().toISOString().slice(0, 7) } = req.query; // YYYY-MM format
    
    const accounts = await db.all(`
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
        
        -- Risk calculation using monthly aggregated data
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
          OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(mm.month || '-01')
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
    `, month);

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
        -- Account must be launched before or during this month
        DATE(a.launched_at) <= DATE(mm.month || '-01', '+1 month', '-1 day')
        AND (
          -- Account is not archived (both fields are null), OR
          (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
          -- Account was archived after this month started (use fallback date if needed)
          OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(mm.month || '-01')
        )
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
        -- Include accounts that were active during this month (same logic as Historical Performance)
        DATE(a.launched_at) <= DATE(mm.month || '-01', '+1 month', '-1 day')
        AND (
          (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
          OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(mm.month || '-01')
        )
      )
      AND mm.month >= strftime('%Y-%m', 'now', '-11 months')
      AND mm.month < strftime('%Y-%m', 'now')
      GROUP BY mm.month
      ORDER BY mm.month ASC
    `);

    res.json(trends);
  } catch (error) {
    console.error('Error fetching monthly trends:', error);
    res.status(500).json({ error: 'Failed to fetch monthly trends data' });
  }
});

// Alias for backward compatibility with frontend
app.get("/api/bigquery/monthly-trends", async (req, res) => {
  // Redirect to the main monthly-trends endpoint
  try {
    const trends = await db.all(`
      SELECT 
        mm.month,
        mm.month_label,
        COUNT(*) as total_accounts,
        
        -- Use live calculation for current month, historical for past months
        SUM(
          CASE 
            WHEN mm.month = strftime('%Y-%m', 'now') THEN
              CASE 
                WHEN a.status = 'FROZEN' THEN 1
                WHEN (mm.avg_active_subs_cnt < 300 AND mm.total_coupons_redeemed < 35) THEN 1
                ELSE 0
              END
            ELSE
              CASE WHEN mm.historical_risk_level = 'high' THEN 1 ELSE 0 END
          END
        ) as high_risk,
        
        SUM(
          CASE 
            WHEN mm.month = strftime('%Y-%m', 'now') THEN
              CASE 
                WHEN a.status = 'FROZEN' THEN 0
                WHEN (mm.avg_active_subs_cnt < 300 AND mm.total_coupons_redeemed < 35) THEN 0
                WHEN (mm.total_coupons_redeemed <= 3 OR mm.avg_active_subs_cnt < 300 OR mm.total_coupons_redeemed < 35) THEN 1
                ELSE 0
              END
            ELSE
              CASE WHEN mm.historical_risk_level = 'medium' THEN 1 ELSE 0 END
          END
        ) as medium_risk,
        
        SUM(
          CASE 
            WHEN mm.month = strftime('%Y-%m', 'now') THEN
              CASE 
                WHEN a.status = 'FROZEN' THEN 0
                WHEN (mm.avg_active_subs_cnt < 300 AND mm.total_coupons_redeemed < 35) THEN 0
                WHEN (mm.total_coupons_redeemed <= 3 OR mm.avg_active_subs_cnt < 300 OR mm.total_coupons_redeemed < 35) THEN 0
                ELSE 1
              END
            ELSE
              CASE WHEN mm.historical_risk_level = 'low' THEN 1 ELSE 0 END
          END
        ) as low_risk
        
      FROM monthly_metrics mm
      INNER JOIN accounts a ON mm.account_id = a.account_id
      WHERE (
        -- Include accounts that were active during this month
        DATE(a.launched_at) <= DATE(mm.month || '-01', '+1 month', '-1 day')
        AND (
          -- Account is not archived (both fields are null), OR
          (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
          -- Account was archived during or after this month started
          OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(mm.month || '-01')
        )
      )
      AND mm.month >= strftime('%Y-%m', 'now', '-12 months')
      AND mm.month <= strftime('%Y-%m', 'now')
      GROUP BY mm.month, mm.month_label
      ORDER BY mm.month ASC
    `);

    // Format to match frontend expectations
    const formattedTrends = trends.map(row => ({
      period: row.month,
      periodLabel: row.month_label,
      month_label: row.month_label,
      month: row.month,
      monthLabel: row.month_label,
      total_accounts: row.total_accounts,
      high_risk: row.high_risk,
      medium_risk: row.medium_risk,
      low_risk: row.low_risk,
      // Add summary object like 2.1 format
      summary: {
        total: row.total_accounts,
        high_risk: row.high_risk,
        medium_risk: row.medium_risk,
        low_risk: row.low_risk
      }
    }));

    res.json(formattedTrends);
  } catch (error) {
    console.error('Error fetching bigquery monthly trends:', error);
    res.status(500).json({ error: 'Failed to fetch monthly trends data' });
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

// Start server
async function start() {
  try {
    await initDatabase();
    
    app.listen(PORT, () => {
      console.log(`🚀 ChurnGuard 2.2 (PostgreSQL Simulation) running at http://localhost:${PORT}`);
      console.log(`📊 Serving data from SQLite simulation database`);
      console.log(`🎯 Ready to test 2.1 UI with PostgreSQL architecture!`);
      console.log(`🔄 Data: ${new Date().toISOString()}`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

start();