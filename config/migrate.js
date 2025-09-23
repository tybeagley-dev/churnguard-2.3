import { getSharedDatabase } from './database.js';

export async function runMigrations() {
  console.log('🔄 Running database migrations...');

  const db = await getSharedDatabase();

  try {
    // Create accounts table
    await db.query(`
      CREATE TABLE IF NOT EXISTS accounts (
        account_id TEXT PRIMARY KEY,
        account_name TEXT,
        status TEXT,
        launched_at TEXT,
        csm_owner TEXT,
        hubspot_id TEXT,
        archived_at TEXT,
        earliest_unit_archived_at TEXT,
        last_updated TEXT
      )
    `);

    // Create daily_metrics table
    await db.query(`
      CREATE TABLE IF NOT EXISTS daily_metrics (
        account_id TEXT,
        date TEXT,
        total_spend REAL DEFAULT 0,
        total_texts_delivered INTEGER DEFAULT 0,
        coupons_redeemed INTEGER DEFAULT 0,
        active_subs_cnt INTEGER DEFAULT 0,
        spend_updated_at TEXT,
        texts_updated_at TEXT,
        coupons_updated_at TEXT,
        subs_updated_at TEXT,
        PRIMARY KEY (account_id, date),
        FOREIGN KEY (account_id) REFERENCES accounts (account_id)
      )
    `);

    // Create monthly_metrics table
    await db.query(`
      CREATE TABLE IF NOT EXISTS monthly_metrics (
        account_id TEXT,
        month TEXT,
        month_label TEXT,
        total_spend REAL DEFAULT 0,
        total_texts_delivered INTEGER DEFAULT 0,
        total_coupons_redeemed INTEGER DEFAULT 0,
        avg_active_subs_cnt REAL DEFAULT 0,
        trending_risk_level TEXT,
        trending_risk_reasons TEXT,
        historical_risk_level TEXT,
        risk_reasons TEXT,
        created_at TEXT,
        updated_at TEXT,
        PRIMARY KEY (account_id, month),
        FOREIGN KEY (account_id) REFERENCES accounts (account_id)
      )
    `);

    // Create indexes for performance
    await db.query(`CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics(date)`);
    await db.query(`CREATE INDEX IF NOT EXISTS idx_daily_metrics_account ON daily_metrics(account_id)`);
    await db.query(`CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status)`);
    await db.query(`CREATE INDEX IF NOT EXISTS idx_monthly_metrics_month ON monthly_metrics(month)`);
    await db.query(`CREATE INDEX IF NOT EXISTS idx_monthly_metrics_account ON monthly_metrics(account_id)`);

    console.log('✅ Database schema migration completed successfully');

    // Verify tables exist
    const result = await db.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      AND table_name IN ('accounts', 'daily_metrics', 'monthly_metrics')
      ORDER BY table_name
    `);

    const tableNames = result.rows.map(row => row.table_name);
    console.log(`📋 Verified tables: ${tableNames.join(', ')}`);

    return true;

  } catch (error) {
    console.error('❌ Migration failed:', error);
    throw error;
  }
}