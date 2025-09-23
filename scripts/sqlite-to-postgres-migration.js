import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

class SQLiteToPostgreSQLMigration {
  constructor() {
    this.sqliteDbPath = './data/churnguard_simulation.db';

    // PostgreSQL connection
    this.pgPool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
    });
  }

  async connectSQLite() {
    console.log('📂 Connecting to SQLite database...');
    return await open({
      filename: this.sqliteDbPath,
      driver: sqlite3.Database
    });
  }

  async migrateAccounts() {
    console.log('👥 Migrating accounts...');

    const sqlite = await this.connectSQLite();
    const accounts = await sqlite.all('SELECT * FROM accounts');

    console.log(`📊 Found ${accounts.length} accounts to migrate`);

    // Clear existing accounts
    await this.pgPool.query('DELETE FROM accounts');

    // Insert accounts in batches
    const batchSize = 100;
    for (let i = 0; i < accounts.length; i += batchSize) {
      const batch = accounts.slice(i, i + batchSize);

      for (const account of batch) {
        await this.pgPool.query(`
          INSERT INTO accounts (
            account_id, account_name, status, launched_at, csm_owner,
            hubspot_id, archived_at, earliest_unit_archived_at, last_updated
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          ON CONFLICT (account_id) DO UPDATE SET
            account_name = EXCLUDED.account_name,
            status = EXCLUDED.status,
            launched_at = EXCLUDED.launched_at,
            csm_owner = EXCLUDED.csm_owner,
            hubspot_id = EXCLUDED.hubspot_id,
            archived_at = EXCLUDED.archived_at,
            earliest_unit_archived_at = EXCLUDED.earliest_unit_archived_at,
            last_updated = EXCLUDED.last_updated
        `, [
          account.account_id, account.account_name, account.status,
          account.launched_at, account.csm_owner, account.hubspot_id,
          account.archived_at, account.earliest_unit_archived_at, account.last_updated
        ]);
      }

      console.log(`   ✅ Migrated ${Math.min(i + batchSize, accounts.length)}/${accounts.length} accounts`);
    }

    await sqlite.close();
    console.log('✅ Accounts migration completed');
  }

  async migrateDailyMetrics() {
    console.log('📈 Migrating daily metrics...');

    const sqlite = await this.connectSQLite();
    const count = await sqlite.get('SELECT COUNT(*) as count FROM daily_metrics');
    console.log(`📊 Found ${count.count} daily metrics records to migrate`);

    // Clear existing daily metrics
    await this.pgPool.query('DELETE FROM daily_metrics');

    // Migrate in chunks to avoid memory issues
    const chunkSize = 5000;
    let offset = 0;
    let totalMigrated = 0;

    while (true) {
      const metrics = await sqlite.all(`
        SELECT * FROM daily_metrics
        ORDER BY account_id, date
        LIMIT ${chunkSize} OFFSET ${offset}
      `);

      if (metrics.length === 0) break;

      for (const metric of metrics) {
        await this.pgPool.query(`
          INSERT INTO daily_metrics (
            account_id, date, total_spend, total_texts_delivered,
            coupons_redeemed, active_subs_cnt, spend_updated_at,
            texts_updated_at, coupons_updated_at, subs_updated_at
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          ON CONFLICT (account_id, date) DO UPDATE SET
            total_spend = EXCLUDED.total_spend,
            total_texts_delivered = EXCLUDED.total_texts_delivered,
            coupons_redeemed = EXCLUDED.coupons_redeemed,
            active_subs_cnt = EXCLUDED.active_subs_cnt,
            spend_updated_at = EXCLUDED.spend_updated_at,
            texts_updated_at = EXCLUDED.texts_updated_at,
            coupons_updated_at = EXCLUDED.coupons_updated_at,
            subs_updated_at = EXCLUDED.subs_updated_at
        `, [
          metric.account_id, metric.date, metric.total_spend || 0,
          metric.total_texts_delivered || 0, metric.coupons_redeemed || 0,
          metric.active_subs_cnt || 0, metric.spend_updated_at,
          metric.texts_updated_at, metric.coupons_updated_at, metric.subs_updated_at
        ]);
      }

      totalMigrated += metrics.length;
      offset += chunkSize;
      console.log(`   ✅ Migrated ${totalMigrated}/${count.count} daily metrics`);
    }

    await sqlite.close();
    console.log('✅ Daily metrics migration completed');
  }

  async migrateMonthlyMetrics() {
    console.log('📅 Migrating monthly metrics...');

    const sqlite = await this.connectSQLite();
    const monthlyMetrics = await sqlite.all('SELECT * FROM monthly_metrics ORDER BY account_id, month');

    console.log(`📊 Found ${monthlyMetrics.length} monthly metrics records to migrate`);

    // Clear existing monthly metrics
    await this.pgPool.query('DELETE FROM monthly_metrics');

    for (const metric of monthlyMetrics) {
      await this.pgPool.query(`
        INSERT INTO monthly_metrics (
          account_id, month, month_label, total_spend, total_texts_delivered,
          total_coupons_redeemed, avg_active_subs_cnt, trending_risk_level,
          trending_risk_reasons, historical_risk_level, risk_reasons,
          created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (account_id, month) DO UPDATE SET
          month_label = EXCLUDED.month_label,
          total_spend = EXCLUDED.total_spend,
          total_texts_delivered = EXCLUDED.total_texts_delivered,
          total_coupons_redeemed = EXCLUDED.total_coupons_redeemed,
          avg_active_subs_cnt = EXCLUDED.avg_active_subs_cnt,
          trending_risk_level = EXCLUDED.trending_risk_level,
          trending_risk_reasons = EXCLUDED.trending_risk_reasons,
          historical_risk_level = EXCLUDED.historical_risk_level,
          risk_reasons = EXCLUDED.risk_reasons,
          created_at = EXCLUDED.created_at,
          updated_at = EXCLUDED.updated_at
      `, [
        metric.account_id, metric.month, metric.month_label,
        metric.total_spend || 0, metric.total_texts_delivered || 0,
        metric.total_coupons_redeemed || 0, metric.avg_active_subs_cnt || 0,
        metric.trending_risk_level, metric.trending_risk_reasons,
        metric.historical_risk_level, metric.risk_reasons,
        metric.created_at, metric.updated_at
      ]);
    }

    await sqlite.close();
    console.log('✅ Monthly metrics migration completed');
  }

  async migrate() {
    try {
      console.log('🚀 Starting SQLite to PostgreSQL migration...');
      console.log('============================================');

      await this.migrateAccounts();
      await this.migrateDailyMetrics();
      await this.migrateMonthlyMetrics();

      console.log('============================================');
      console.log('🎉 Migration completed successfully!');

      // Verify migration
      const accountsResult = await this.pgPool.query('SELECT COUNT(*) as count FROM accounts');
      const dailyResult = await this.pgPool.query('SELECT COUNT(*) as count FROM daily_metrics');
      const monthlyResult = await this.pgPool.query('SELECT COUNT(*) as count FROM monthly_metrics');

      console.log('📊 Final counts:');
      console.log(`   Accounts: ${accountsResult.rows[0].count}`);
      console.log(`   Daily metrics: ${dailyResult.rows[0].count}`);
      console.log(`   Monthly metrics: ${monthlyResult.rows[0].count}`);

      return {
        success: true,
        accounts: accountsResult.rows[0].count,
        dailyMetrics: dailyResult.rows[0].count,
        monthlyMetrics: monthlyResult.rows[0].count
      };

    } catch (error) {
      console.error('❌ Migration failed:', error);
      throw error;
    } finally {
      await this.pgPool.end();
    }
  }
}

// Run migration if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const migration = new SQLiteToPostgreSQLMigration();
  migration.migrate()
    .then(result => {
      console.log('✅ Migration script completed successfully');
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Migration script failed:', error);
      process.exit(1);
    });
}

export { SQLiteToPostgreSQLMigration };