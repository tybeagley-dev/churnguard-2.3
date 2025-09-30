import { BigQuery } from '@google-cloud/bigquery';
import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

class DailyMetricsETLPostgresNative {
  constructor() {
    // BigQuery client with proper credential handling
    const bigqueryConfig = {
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
    };

    // Handle credentials: use JSON string in production, file path in development
    if (process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) {
      try {
        bigqueryConfig.credentials = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON);
      } catch (error) {
        console.error('❌ Failed to parse GOOGLE_APPLICATION_CREDENTIALS_JSON:', error);
        throw new Error('Invalid BigQuery credentials JSON');
      }
    } else if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      bigqueryConfig.keyFilename = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    } else {
      console.error('❌ No BigQuery credentials found. Set GOOGLE_APPLICATION_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS');
      throw new Error('Missing BigQuery credentials');
    }

    this.bigquery = new BigQuery(bigqueryConfig);

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
      statement_timeout: 300000,  // 5 minute query timeout
      query_timeout: 300000,      // 5 minute query timeout
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

  // Get all daily metrics data for a date (combining all 4 SQLite ETL queries)
  async getAllDailyMetricsForDate(date) {
    console.log(`📊 Fetching all daily metrics for ${date}...`);

    // Combined query that gets all 4 metric types in one BigQuery call
    const query = `
      WITH account_unit_archive_dates AS (
        SELECT
          u.account_id,
          MIN(u.archived_at) as earliest_unit_archived_at
        FROM units.units u
        WHERE u.status = 'ARCHIVED' AND u.archived_at IS NOT NULL
        GROUP BY u.account_id
      ),

      eligible_accounts AS (
        SELECT DISTINCT a.id as account_id
        FROM accounts.accounts a
        LEFT JOIN account_unit_archive_dates aad ON a.id = aad.account_id
        WHERE (
            -- Include accounts that have launched on or before the processing date
            (a.launched_at IS NOT NULL AND DATE(a.launched_at) <= DATE('${date}'))
            OR
            -- Include accounts launched within the same month (captures pre-launch platform fees on 1st of month)
            (a.launched_at IS NOT NULL AND DATE_TRUNC('month', DATE(a.launched_at)) = DATE_TRUNC('month', DATE('${date}')))
            OR
            -- Include accounts with NULL launch date that have revenue activity (checked via revenue CTE join below)
            (a.launched_at IS NULL)
          )
          AND (
            -- Account is not archived at all
            (a.archived_at IS NULL AND aad.earliest_unit_archived_at IS NULL)
            OR
            -- Account was archived but the date we're processing is BEFORE the archive date
            -- This ensures we collect metrics for all days up to (but not including) the archive date
            (COALESCE(a.archived_at, aad.earliest_unit_archived_at) IS NOT NULL
             AND DATE('${date}') < DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)))
          )
          -- ENHANCEMENT: Exclude accounts that are archived but have no archive date
          -- (assume they're old and outside our 12-month window)
          AND NOT (
            a.status = 'ARCHIVED'
            AND a.archived_at IS NULL
            AND aad.earliest_unit_archived_at IS NULL
          )
      ),

      -- Spend data
      spend_metrics AS (
        SELECT
          tr.account_id,
          SUM(tr.total) as total_spend
        FROM dbt_models.total_revenue_by_account_and_date tr
        INNER JOIN eligible_accounts ea ON tr.account_id = ea.account_id
        WHERE tr.date = DATE('${date}')
        GROUP BY tr.account_id
        HAVING SUM(tr.total) > 0
      ),

      -- Text message data (IDENTICAL to SQLite version)
      text_metrics AS (
        SELECT
          u.account_id,
          COUNT(t.id) as total_texts_delivered
        FROM public.texts t
        JOIN units.units u ON u.id = t.unit_id
        INNER JOIN eligible_accounts ea ON u.account_id = ea.account_id
        WHERE DATE(t.created_at) = DATE('${date}')
        GROUP BY u.account_id
        HAVING COUNT(t.id) > 0
      ),

      -- Coupon data (IDENTICAL to SQLite version)
      coupon_metrics AS (
        SELECT
          u.account_id,
          COUNT(c.id) as coupons_redeemed
        FROM promos.coupons c
        JOIN units.units u ON u.id = c.unit_id
        INNER JOIN eligible_accounts ea ON u.account_id = ea.account_id
        WHERE DATE(c.redeemed_at) = DATE('${date}')
          AND c.redeemed_at IS NOT NULL
        GROUP BY u.account_id
        HAVING COUNT(c.id) > 0
      ),

      -- Subscription data (IDENTICAL to SQLite version)
      sub_metrics AS (
        SELECT
          u.account_id,
          COUNT(DISTINCT s.id) as active_subs_cnt
        FROM public.subscriptions s
        JOIN units.units u ON s.channel_id = u.id
        INNER JOIN eligible_accounts ea ON u.account_id = ea.account_id
        WHERE DATE(s.created_at) <= DATE('${date}')
          AND (s.deactivated_at IS NULL OR DATE(s.deactivated_at) > DATE('${date}'))
        GROUP BY u.account_id
        HAVING COUNT(DISTINCT s.id) > 0
      )

      -- Combine all metrics with FULL OUTER JOIN to get complete picture
      SELECT
        COALESCE(ea.account_id, s.account_id, t.account_id, c.account_id, sub.account_id) as account_id,
        COALESCE(s.total_spend, 0) as total_spend,
        COALESCE(t.total_texts_delivered, 0) as total_texts_delivered,
        COALESCE(c.coupons_redeemed, 0) as coupons_redeemed,
        COALESCE(sub.active_subs_cnt, 0) as active_subs_cnt
      FROM eligible_accounts ea
      FULL OUTER JOIN spend_metrics s ON ea.account_id = s.account_id
      FULL OUTER JOIN text_metrics t ON ea.account_id = t.account_id
      FULL OUTER JOIN coupon_metrics c ON ea.account_id = c.account_id
      FULL OUTER JOIN sub_metrics sub ON ea.account_id = sub.account_id
      WHERE (
        COALESCE(s.total_spend, 0) > 0 OR
        COALESCE(t.total_texts_delivered, 0) > 0 OR
        COALESCE(c.coupons_redeemed, 0) > 0 OR
        COALESCE(sub.active_subs_cnt, 0) > 0
      )
      ORDER BY account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found metrics for ${rows.length} accounts with activity on ${date}`);
    return rows;
  }

  // PostgreSQL-native batch upsert with proper error handling
  async updateMetricsTable(date, metricsData, dryRun = false) {
    if (dryRun) {
      console.log(`🧪 DRY RUN: Would update daily metrics for ${date} (${metricsData.length} accounts)...`);
      console.log(`🧪 Sample data:`, metricsData.slice(0, 3));
      return { updatedCount: 0, createdCount: 0, totalProcessed: metricsData.length, dryRun: true };
    }

    console.log(`📊 Updating daily metrics for ${date} (${metricsData.length} accounts)...`);

    const client = await this.pool.connect();
    let updatedCount = 0;
    let createdCount = 0;

    try {
      await client.query('BEGIN');

      // Batch process in chunks of 100 to avoid memory issues
      const chunkSize = 100;
      for (let i = 0; i < metricsData.length; i += chunkSize) {
        const chunk = metricsData.slice(i, i + chunkSize);

        for (const metrics of chunk) {
          // IDENTICAL logic to SQLite: Try UPDATE first, then INSERT
          const updateResult = await client.query(`
            UPDATE daily_metrics
            SET
              total_spend = $1,
              total_texts_delivered = $2,
              coupons_redeemed = $3,
              active_subs_cnt = $4,
              spend_updated_at = $5,
              texts_updated_at = $6,
              coupons_updated_at = $7,
              subs_updated_at = $8
            WHERE account_id = $9 AND date = $10
          `, [
            parseFloat(metrics.total_spend) || 0,
            parseInt(metrics.total_texts_delivered) || 0,
            parseInt(metrics.coupons_redeemed) || 0,
            parseInt(metrics.active_subs_cnt) || 0,
            new Date().toISOString(),  // IDENTICAL timestamp format
            new Date().toISOString(),
            new Date().toISOString(),
            new Date().toISOString(),
            metrics.account_id,
            date
          ]);

          if (updateResult.rowCount > 0) {
            updatedCount++;
          } else {
            // IDENTICAL logic: Insert if no existing row
            await client.query(`
              INSERT INTO daily_metrics (
                account_id, date, total_spend, total_texts_delivered,
                coupons_redeemed, active_subs_cnt, spend_updated_at,
                texts_updated_at, coupons_updated_at, subs_updated_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            `, [
              metrics.account_id,
              date,
              parseFloat(metrics.total_spend) || 0,
              parseInt(metrics.total_texts_delivered) || 0,
              parseInt(metrics.coupons_redeemed) || 0,
              parseInt(metrics.active_subs_cnt) || 0,
              new Date().toISOString(),  // IDENTICAL timestamp format
              new Date().toISOString(),
              new Date().toISOString(),
              new Date().toISOString()
            ]);
            createdCount++;
          }
        }

        // Log progress for large batches
        if (metricsData.length > chunkSize) {
          console.log(`   📈 Processed ${Math.min(i + chunkSize, metricsData.length)}/${metricsData.length} accounts...`);
        }
      }

      await client.query('COMMIT');

      // IDENTICAL console output and return format
      console.log(`✅ Daily metrics ETL for ${date}: ${updatedCount} updated, ${createdCount} created`);
      return { updatedCount, createdCount, totalProcessed: metricsData.length };

    } catch (error) {
      await client.query('ROLLBACK');
      console.error(`❌ Daily metrics ETL failed for ${date}:`, error);
      throw error;
    } finally {
      client.release();
    }
  }

  async processDate(date, options = {}) {
    const { dryRun = false, testConnection = false } = options;

    if (testConnection) {
      await this.testConnection();
      if (testConnection === 'only') {
        return { connectionTest: true, success: true };
      }
    }

    const metricsData = await this.getAllDailyMetricsForDate(date);
    const result = await this.updateMetricsTable(date, metricsData, dryRun);
    return result;
  }

  // Clean shutdown
  async close() {
    await this.pool.end();
    console.log('🔌 PostgreSQL connection pool closed');
  }
}

// IDENTICAL CLI behavior to SQLite versions with test/dry-run options
if (import.meta.url === `file://${process.argv[1]}`) {
  const date = process.argv[2];
  const flags = process.argv.slice(3);

  const dryRun = flags.includes('--dry-run');
  const testConnection = flags.includes('--test-connection') ? 'only' : false;

  if (!date && !testConnection) {
    console.error(`❌ Usage: node daily-metrics-etl-postgres-native.js YYYY-MM-DD [options]

Options:
  --dry-run           Fetch data but don't write to database
  --test-connection   Test PostgreSQL connection only

Examples:
  node daily-metrics-etl-postgres-native.js 2025-09-24
  node daily-metrics-etl-postgres-native.js 2025-09-24 --dry-run
  node daily-metrics-etl-postgres-native.js --test-connection
`);
    process.exit(1);
  }

  const etl = new DailyMetricsETLPostgresNative();
  etl.processDate(date, { dryRun, testConnection })
    .then(result => {
      if (result.connectionTest) {
        console.log('🎉 PostgreSQL connection test passed!');
      } else if (result.dryRun) {
        console.log(`🧪 Dry run completed for ${date}!`);
        console.log(`📊 Summary: ${result.totalProcessed} accounts would be processed`);
      } else {
        console.log(`🎉 Daily metrics ETL completed for ${date}!`);
        console.log(`📊 Summary: ${result.totalProcessed} accounts processed, ${result.updatedCount} updated, ${result.createdCount} created`);
      }
      return etl.close();
    })
    .then(() => {
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Daily metrics ETL failed:', error);
      etl.close().finally(() => process.exit(1));
    });
}

export { DailyMetricsETLPostgresNative };