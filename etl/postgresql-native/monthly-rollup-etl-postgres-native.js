import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

class MonthlyRollupETLPostgresNative {
  constructor() {
    // Initialize PostgreSQL pool with proper SSL configuration
    if (!process.env.DATABASE_URL) {
      throw new Error('DATABASE_URL environment variable is required');
    }

    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: {
        rejectUnauthorized: false,
        sslmode: 'require'
      },
      max: 10,                    // Maximum connections in pool
      idleTimeoutMillis: 30000,   // Close idle connections after 30s
      connectionTimeoutMillis: 10000,  // Wait 10s for connection
      statement_timeout: 600000,  // 10 minute query timeout (monthly aggregation is heavy)
      query_timeout: 600000,      // 10 minute query timeout
      keepAlive: true,
      keepAliveInitialDelayMillis: 10000
    });

    // Test connection on startup
    this.pool.on('error', (err) => {
      console.error('❌ PostgreSQL pool error:', err);
    });
  }

  async testConnection() {
    const client = await this.pool.connect();
    try {
      const result = await client.query('SELECT NOW() as current_time');
      console.log(`✅ PostgreSQL connected at ${result.rows[0].current_time}`);
    } catch (error) {
      console.error('❌ PostgreSQL connection test failed:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  // Get month details in IDENTICAL format to SQLite version
  getMonthDetails(month = null) {
    const targetMonth = month || new Date().toISOString().slice(0, 7); // YYYY-MM format
    const monthLabel = new Date(targetMonth + '-01').toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      timeZone: 'UTC'
    });

    return { targetMonth, monthLabel };
  }

  async updateMonthlyMetrics(month = null) {
    const { targetMonth, monthLabel } = this.getMonthDetails(month);

    console.log(`🔄 Updating monthly metrics for ${monthLabel} (${targetMonth})...`);
    console.log(`⚠️  WARNING: This will DELETE and recreate ALL monthly_metrics for ${monthLabel}`);
    console.log(`⚠️  WARNING: This will WIPE risk_reasons, trending_risk_level, and historical_risk_level data!`);

    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      // Step 1: Delete existing monthly data for this month (IDENTICAL to SQLite logic)
      console.log(`🗑️  Deleting existing monthly_metrics for ${targetMonth}...`);
      const deleteResult = await client.query(
        'DELETE FROM monthly_metrics WHERE month = $1',
        [targetMonth]
      );
      console.log(`✅ Deleted ${deleteResult.rowCount} existing monthly metric rows`);

      // Step 2: Recreate monthly data from daily_metrics (IDENTICAL aggregation logic to SQLite)
      console.log(`📈 Aggregating daily metrics to monthly for ${targetMonth}...`);

      const insertResult = await client.query(`
        INSERT INTO monthly_metrics (
          account_id, month, month_label, total_spend, total_texts_delivered,
          total_coupons_redeemed, avg_active_subs_cnt, days_with_activity,
          last_updated
        )
        SELECT
          dm.account_id,
          $1::text as month,
          $2::text as month_label,
          SUM(dm.total_spend) as total_spend,
          SUM(dm.total_texts_delivered) as total_texts_delivered,
          SUM(dm.coupons_redeemed) as total_coupons_redeemed,
          ROUND(AVG(dm.active_subs_cnt)) as avg_active_subs_cnt,
          COUNT(DISTINCT dm.date) as days_with_activity,
          NOW() as last_updated
        FROM accounts a
        LEFT JOIN daily_metrics dm ON a.account_id = dm.account_id
          AND TO_CHAR(dm.date::date, 'YYYY-MM') = $3
        WHERE (
          -- IDENTICAL filtering logic to SQLite version
          a.launched_at::date <= (($4 || '-01')::date + INTERVAL '1 month' - INTERVAL '1 day')::date
          AND (
            (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
            OR COALESCE(a.archived_at, a.earliest_unit_archived_at)::date >= ($5 || '-01')::date
          )
        )
        GROUP BY a.account_id
      `, [
        targetMonth,     // $1: month
        monthLabel,      // $2: month_label
        targetMonth,     // $3: date filter for daily_metrics
        targetMonth,     // $4: launched_at comparison
        targetMonth      // $5: archived_at comparison
      ]);

      console.log(`✅ Created ${insertResult.rowCount} monthly metric rows for ${monthLabel}`);

      await client.query('COMMIT');

      // IDENTICAL return format to SQLite version
      return {
        month: targetMonth,
        monthLabel: monthLabel,
        accountsProcessed: insertResult.rowCount
      };

    } catch (error) {
      await client.query('ROLLBACK');
      console.error(`❌ Monthly rollup ETL failed for ${targetMonth}:`, error);
      throw error;
    } finally {
      client.release();
    }
  }

  // Process a specific month or current month by default
  async processMonth(month = null) {
    await this.testConnection();
    const result = await this.updateMonthlyMetrics(month);
    return result;
  }

  // Clean shutdown
  async close() {
    await this.pool.end();
    console.log('🔌 PostgreSQL connection pool closed');
  }
}

// IDENTICAL CLI behavior to SQLite update-current-month.js
if (import.meta.url === `file://${process.argv[1]}`) {
  const month = process.argv[2]; // Optional: specify month as YYYY-MM, defaults to current month

  if (month && !/^\d{4}-\d{2}$/.test(month)) {
    console.error('❌ Usage: node monthly-rollup-etl-postgres-native.js [YYYY-MM]');
    console.error('   Example: node monthly-rollup-etl-postgres-native.js 2025-09');
    console.error('   If no month specified, uses current month');
    process.exit(1);
  }

  const etl = new MonthlyRollupETLPostgresNative();
  etl.processMonth(month)
    .then(result => {
      console.log(`🎉 Monthly rollup ETL completed for ${result.monthLabel}!`);
      console.log(`📊 Summary: ${result.accountsProcessed} accounts processed for ${result.month}`);
      return etl.close();
    })
    .then(() => {
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Monthly rollup ETL failed:', error);
      etl.close().finally(() => process.exit(1));
    });
}

export { MonthlyRollupETLPostgresNative };