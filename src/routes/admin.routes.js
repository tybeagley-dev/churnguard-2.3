import express from 'express';

const router = express.Router();

// One-time data sync endpoint
router.post('/sync-data', async (req, res) => {
  try {
    console.log('🔄 Starting BigQuery to PostgreSQL data sync...');

    // Import the BigQuery ETL class
    const { BigQueryDataRetrieval } = await import('../../etl/bigquery-data-retrieval.js');

    const etl = new BigQueryDataRetrieval();

    console.log('🔄 Running full BigQuery to PostgreSQL ETL...');
    const result = await etl.runFullRetrieval();

    if (result.success) {
      console.log('✅ BigQuery to PostgreSQL sync completed successfully!');
    } else {
      throw new Error(`ETL failed: ${result.error || 'Unknown error'}`);
    }

    res.json({
      success: true,
      message: 'BigQuery data sync completed successfully',
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