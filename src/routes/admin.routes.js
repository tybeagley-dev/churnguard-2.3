import express from 'express';

const router = express.Router();

// Daily production ETL endpoint
router.post('/sync-data', async (req, res) => {
  try {
    console.log('🔄 Starting Daily Production ETL...');

    // Get target date from request body (defaults to yesterday)
    const { date } = req.body;

    // Import the PostgreSQL Daily Production ETL class
    const { DailyProductionETLPostgreSQL } = await import('../../etl/daily-production-etl-postgresql.js');

    const etl = new DailyProductionETLPostgreSQL();

    console.log(`🔄 Running Daily Production ETL${date ? ` for ${date}` : ' (yesterday)'}...`);
    const result = await etl.runDailyETL(date);

    if (result.success) {
      console.log('✅ Daily Production ETL completed successfully!');
    } else {
      throw new Error(`ETL failed: ${result.error || 'Unknown error'}`);
    }

    res.json({
      success: true,
      message: 'Daily Production ETL completed successfully',
      processDate: result.processDate,
      duration: result.duration,
      results: {
        accounts: result.accountsResults,
        extraction: result.extractResults,
        monthly: result.monthlyResults,
        risk: result.riskResults
      },
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('❌ Data sync failed:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Check sync status
router.get('/sync-status', async (req, res) => {
  try {
    const { getSharedDatabase } = await import('../../config/database.js');
    const db = await getSharedDatabase();

    // Get record counts
    const accountsResult = await db.query('SELECT COUNT(*) as count FROM accounts');
    const dailyResult = await db.query('SELECT COUNT(*) as count FROM daily_metrics');
    const monthlyResult = await db.query('SELECT COUNT(*) as count FROM monthly_metrics');

    res.json({
      accounts: accountsResult.rows[0].count,
      daily_metrics: dailyResult.rows[0].count,
      monthly_metrics: monthlyResult.rows[0].count,
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