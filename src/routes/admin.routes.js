import express from 'express';

const router = express.Router();

// Master ETL endpoint - orchestrates all modular steps
router.post('/sync-data', async (req, res) => {
  try {
    console.log('🔄 Starting Modular Daily Production ETL...');

    const { date } = req.body;
    const targetDate = date || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    console.log(`🔄 Running Complete Daily Production ETL for ${targetDate}...`);

    const startTime = Date.now();
    const results = {};

    // Step 1: Accounts
    console.log('🚀 Step 1: Syncing accounts...');
    const accountsResponse = await fetch(`${process.env.BASE_URL || 'http://localhost:10000'}/api/admin/sync-accounts`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ date: targetDate })
    });

    if (!accountsResponse.ok) {
      const error = await accountsResponse.json();
      throw new Error(`Accounts sync failed: ${error.error}`);
    }

    results.accounts = await accountsResponse.json();
    console.log('✅ Step 1 complete: Accounts synced');

    // Step 2: Daily Metrics
    console.log('🚀 Step 2: Syncing daily metrics...');
    const dailyResponse = await fetch(`${process.env.BASE_URL || 'http://localhost:10000'}/api/admin/sync-daily`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ date: targetDate })
    });

    if (!dailyResponse.ok) {
      const error = await dailyResponse.json();
      throw new Error(`Daily metrics sync failed: ${error.error}`);
    }

    results.daily = await dailyResponse.json();
    console.log('✅ Step 2 complete: Daily metrics synced');

    // Step 3: Monthly Metrics
    console.log('🚀 Step 3: Syncing monthly metrics...');
    const monthlyResponse = await fetch(`${process.env.BASE_URL || 'http://localhost:10000'}/api/admin/sync-monthly`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ date: targetDate })
    });

    if (!monthlyResponse.ok) {
      const error = await monthlyResponse.json();
      throw new Error(`Monthly metrics sync failed: ${error.error}`);
    }

    results.monthly = await monthlyResponse.json();
    console.log('✅ Step 3 complete: Monthly metrics synced');

    const duration = Date.now() - startTime;
    console.log(`🎉 Complete Daily Production ETL finished in ${duration}ms`);

    res.json({
      success: true,
      message: 'Complete Daily Production ETL completed successfully',
      processDate: targetDate,
      duration,
      results,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('❌ Modular ETL failed:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Modular ETL: Sync Accounts Only
router.post('/sync-accounts', async (req, res) => {
  try {
    const { date } = req.body;
    const targetDate = date || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0]; // Default to yesterday

    console.log(`👥 Starting Accounts ETL for ${targetDate}...`);

    const { ETLTracker } = await import('../../etl/shared-scripts/etl-tracker.js');
    const tracker = new ETLTracker();

    // Validate and start step
    await tracker.validateCanRun(targetDate, 'accounts');
    await tracker.startStep(targetDate, 'accounts');

    const { AccountsETLPostgreSQL } = await import('../../etl/postgresql-experimental/accounts-etl-postgresql.js');
    const accountsETL = new AccountsETLPostgreSQL();

    const result = await accountsETL.populateAccounts();

    await tracker.completeStep(targetDate, 'accounts', {
      accountsProcessed: result.accountsProcessed
    });

    res.json({
      success: true,
      message: 'Accounts ETL completed successfully',
      date: targetDate,
      result,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('❌ Accounts ETL failed:', error);

    const { date } = req.body;
    const targetDate = date || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    try {
      const { ETLTracker } = await import('../../etl/shared-scripts/etl-tracker.js');
      const tracker = new ETLTracker();
      await tracker.failStep(targetDate, 'accounts', error.message);
    } catch (trackingError) {
      console.error('❌ Failed to update tracking:', trackingError);
    }

    res.status(500).json({
      success: false,
      error: error.message,
      date: targetDate,
      timestamp: new Date().toISOString()
    });
  }
});

// Modular ETL: Sync Daily Metrics Only (requires accounts complete)
router.post('/sync-daily', async (req, res) => {
  try {
    const { date } = req.body;
    const targetDate = date || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    console.log(`📊 Starting Daily Metrics ETL for ${targetDate}...`);

    const { ETLTracker } = await import('../../etl/shared-scripts/etl-tracker.js');
    const tracker = new ETLTracker();

    // Validate and start step
    await tracker.validateCanRun(targetDate, 'daily');
    await tracker.startStep(targetDate, 'daily');

    const { DailyMetricsETLPostgresNative } = await import('../../etl/postgresql-native/daily-metrics-etl-postgres-native.js');
    const etl = new DailyMetricsETLPostgresNative();

    const result = await etl.processDate(targetDate);

    await tracker.completeStep(targetDate, 'daily', {
      recordsProcessed: result.totalProcessed,
      updatedCount: result.updatedCount,
      createdCount: result.createdCount
    });

    res.json({
      success: true,
      message: 'Daily Metrics ETL completed successfully',
      date: targetDate,
      result,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('❌ Daily Metrics ETL failed:', error);

    const { date } = req.body;
    const targetDate = date || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    try {
      const { ETLTracker } = await import('../../etl/shared-scripts/etl-tracker.js');
      const tracker = new ETLTracker();
      await tracker.failStep(targetDate, 'daily', error.message);
    } catch (trackingError) {
      console.error('❌ Failed to update tracking:', trackingError);
    }

    res.status(500).json({
      success: false,
      error: error.message,
      date: targetDate,
      timestamp: new Date().toISOString()
    });
  }
});

// Modular ETL: Sync Monthly Metrics Only (requires accounts and daily complete)
router.post('/sync-monthly', async (req, res) => {
  try {
    const { date } = req.body;
    const targetDate = date || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    console.log(`📈 Starting Monthly Metrics ETL for ${targetDate}...`);

    const { ETLTracker } = await import('../../etl/shared-scripts/etl-tracker.js');
    const tracker = new ETLTracker();

    // Validate and start step
    await tracker.validateCanRun(targetDate, 'monthly');
    await tracker.startStep(targetDate, 'monthly');

    const { MonthlyRollupETLPostgresNative } = await import('../../etl/postgresql-native/monthly-rollup-etl-postgres-native.js');
    const etl = new MonthlyRollupETLPostgresNative();

    // Get month from date for monthly rollup
    const targetMonth = targetDate.slice(0, 7); // Convert YYYY-MM-DD to YYYY-MM
    const monthlyResult = await etl.processMonth(targetMonth);

    // Note: Risk analysis will be handled separately in future update
    const riskResult = { updatedCount: 0 };

    await tracker.completeStep(targetDate, 'monthly', {
      monthsUpdated: monthlyResult.accountsProcessed,
      trendingRiskUpdated: riskResult.updatedCount
    });

    res.json({
      success: true,
      message: 'Monthly Metrics ETL completed successfully',
      date: targetDate,
      result: {
        monthly: monthlyResult,
        risk: riskResult
      },
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('❌ Monthly Metrics ETL failed:', error);

    const { date } = req.body;
    const targetDate = date || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    try {
      const { ETLTracker } = await import('../../etl/shared-scripts/etl-tracker.js');
      const tracker = new ETLTracker();
      await tracker.failStep(targetDate, 'monthly', error.message);
    } catch (trackingError) {
      console.error('❌ Failed to update tracking:', trackingError);
    }

    res.status(500).json({
      success: false,
      error: error.message,
      date: targetDate,
      timestamp: new Date().toISOString()
    });
  }
});

// Check sync status with ETL tracking
router.get('/sync-status', async (req, res) => {
  try {
    const { date } = req.query;
    const targetDate = date || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    const { getSharedDatabase } = await import('../../config/database.js');
    const db = await getSharedDatabase();

    // Get record counts
    const accountsResult = await db.query('SELECT COUNT(*) as count FROM accounts');
    const dailyResult = await db.query('SELECT COUNT(*) as count FROM daily_metrics');
    const monthlyResult = await db.query('SELECT COUNT(*) as count FROM monthly_metrics');

    // Get ETL tracking status
    const { ETLTracker } = await import('../../etl/shared-scripts/etl-tracker.js');
    const tracker = new ETLTracker();
    const etlStatus = await tracker.getDateStatus(targetDate);

    res.json({
      accounts: accountsResult.rows[0].count,
      daily_metrics: dailyResult.rows[0].count,
      monthly_metrics: monthlyResult.rows[0].count,
      etl_status: etlStatus,
      date: targetDate,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('❌ Status check failed:', error);
    res.status(500).json({
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

export default router;