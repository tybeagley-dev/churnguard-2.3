import { BigQuery } from '@google-cloud/bigquery';
import pkg from 'pg';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import path from 'path';

const { Pool } = pkg;

// Load environment variables from project root
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.join(__dirname, '../..');
dotenv.config({ path: path.join(projectRoot, '.env') });

// Set DATABASE_URL if not already set
if (!process.env.DATABASE_URL && process.env.EXTERNAL_DATABASE_URL) {
  process.env.DATABASE_URL = process.env.EXTERNAL_DATABASE_URL;
}

class MonthlyBackfillOptimized {
  constructor() {
    // BigQuery client
    const bigqueryConfig = {
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
    };

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
      throw new Error('Missing BigQuery credentials');
    }

    this.bigquery = new BigQuery(bigqueryConfig);

    // PostgreSQL pool
    if (!process.env.DATABASE_URL) {
      throw new Error('DATABASE_URL environment variable is required');
    }

    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: {
        rejectUnauthorized: false,
        sslmode: 'require'
      },
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
      statement_timeout: 600000,  // 10 minute timeout for large batches
      query_timeout: 600000,
      keepAlive: true,
      keepAliveInitialDelayMillis: 10000
    });

    this.pool.on('error', (err) => {
      console.error('❌ PostgreSQL pool error:', err);
    });
  }

  // Fetch entire month of data in ONE BigQuery query
  async getAllDailyMetricsForMonth(month) {
    console.log(`📊 Fetching all daily metrics for ${month}...`);

    const startDate = `${month}-01`;
    const endDate = `${month}-${new Date(month + '-01').getDate() === 30 ? '30' : new Date(new Date(month + '-01').getFullYear(), new Date(month + '-01').getMonth() + 1, 0).getDate()}`;

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
        SELECT DISTINCT
          a.id as account_id,
          a.launched_at
        FROM accounts.accounts a
        LEFT JOIN account_unit_archive_dates aad ON a.id = aad.account_id
        WHERE (
            (a.launched_at IS NOT NULL AND DATE(a.launched_at) <= DATE('${endDate}'))
            OR
            (a.launched_at IS NOT NULL AND FORMAT_DATE('%Y-%m', DATE(a.launched_at)) = '${month}')
            OR
            (a.launched_at IS NULL)
          )
          AND (
            a.status != 'ARCHIVED'
            OR
            (a.status = 'ARCHIVED'
             AND COALESCE(a.archived_at, aad.earliest_unit_archived_at) IS NOT NULL
             AND DATE('${endDate}') < DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)))
          )
          AND NOT (
            a.status = 'ARCHIVED'
            AND a.archived_at IS NULL
            AND aad.earliest_unit_archived_at IS NULL
          )
      ),

      -- Generate all dates in the month
      date_series AS (
        SELECT date
        FROM UNNEST(GENERATE_DATE_ARRAY(DATE('${startDate}'), DATE('${endDate}'))) AS date
      ),

      -- Cross join eligible accounts with all dates
      account_dates AS (
        SELECT ea.account_id, ea.launched_at, ds.date
        FROM eligible_accounts ea
        CROSS JOIN date_series ds
      ),

      -- Spend data for the entire month
      spend_metrics AS (
        SELECT
          tr.account_id,
          tr.date,
          SUM(tr.total) as total_spend
        FROM dbt_models.total_revenue_by_account_and_date tr
        INNER JOIN eligible_accounts ea ON tr.account_id = ea.account_id
        WHERE tr.date >= DATE('${startDate}') AND tr.date <= DATE('${endDate}')
        GROUP BY tr.account_id, tr.date
        HAVING SUM(tr.total) > 0
      ),

      -- Text metrics for the entire month
      text_metrics AS (
        SELECT
          u.account_id,
          DATE(t.created_at) as date,
          COUNT(t.id) as total_texts_delivered
        FROM public.texts t
        JOIN units.units u ON u.id = t.unit_id
        INNER JOIN eligible_accounts ea ON u.account_id = ea.account_id
        WHERE DATE(t.created_at) >= DATE('${startDate}')
          AND DATE(t.created_at) <= DATE('${endDate}')
        GROUP BY u.account_id, DATE(t.created_at)
        HAVING COUNT(t.id) > 0
      ),

      -- Coupon metrics for the entire month
      coupon_metrics AS (
        SELECT
          u.account_id,
          DATE(c.redeemed_at) as date,
          COUNT(c.id) as coupons_redeemed
        FROM promos.coupons c
        JOIN units.units u ON u.id = c.unit_id
        INNER JOIN eligible_accounts ea ON u.account_id = ea.account_id
        WHERE DATE(c.redeemed_at) >= DATE('${startDate}')
          AND DATE(c.redeemed_at) <= DATE('${endDate}')
          AND c.redeemed_at IS NOT NULL
        GROUP BY u.account_id, DATE(c.redeemed_at)
        HAVING COUNT(c.id) > 0
      ),

      -- Subscription metrics for the entire month
      sub_metrics AS (
        SELECT
          u.account_id,
          ds.date,
          COUNT(DISTINCT s.id) as active_subs_cnt
        FROM date_series ds
        CROSS JOIN eligible_accounts ea
        JOIN units.units u ON ea.account_id = u.account_id
        JOIN public.subscriptions s ON s.channel_id = u.id
        WHERE DATE(s.created_at) <= ds.date
          AND (s.deactivated_at IS NULL OR DATE(s.deactivated_at) > ds.date)
        GROUP BY u.account_id, ds.date
        HAVING COUNT(DISTINCT s.id) > 0
      )

      -- Combine all metrics
      SELECT
        ad.account_id,
        ad.date,
        ad.launched_at,
        COALESCE(s.total_spend, 0) as total_spend,
        COALESCE(t.total_texts_delivered, 0) as total_texts_delivered,
        COALESCE(c.coupons_redeemed, 0) as coupons_redeemed,
        COALESCE(sub.active_subs_cnt, 0) as active_subs_cnt
      FROM account_dates ad
      LEFT JOIN spend_metrics s ON ad.account_id = s.account_id AND ad.date = s.date
      LEFT JOIN text_metrics t ON ad.account_id = t.account_id AND ad.date = t.date
      LEFT JOIN coupon_metrics c ON ad.account_id = c.account_id AND ad.date = c.date
      LEFT JOIN sub_metrics sub ON ad.account_id = sub.account_id AND ad.date = sub.date
      WHERE (
        ad.launched_at IS NOT NULL
        OR
        (ad.launched_at IS NULL AND COALESCE(s.total_spend, 0) > 0)
      )
      ORDER BY ad.date, ad.account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found ${rows.length} account-day records for ${month}`);
    return rows;
  }

  // Batch upsert all metrics for the month
  async batchUpsertMetrics(month, metricsData) {
    console.log(`📊 Batch upserting ${metricsData.length} records for ${month}...`);

    const client = await this.pool.connect();
    let updatedCount = 0;
    let createdCount = 0;

    try {
      await client.query('BEGIN');

      // Process in chunks of 500
      const chunkSize = 500;
      for (let i = 0; i < metricsData.length; i += chunkSize) {
        const chunk = metricsData.slice(i, i + chunkSize);

        for (const metrics of chunk) {
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
            new Date().toISOString(),
            new Date().toISOString(),
            new Date().toISOString(),
            new Date().toISOString(),
            metrics.account_id,
            metrics.date.value
          ]);

          if (updateResult.rowCount > 0) {
            updatedCount++;
          } else {
            await client.query(`
              INSERT INTO daily_metrics (
                account_id, date, total_spend, total_texts_delivered,
                coupons_redeemed, active_subs_cnt, spend_updated_at,
                texts_updated_at, coupons_updated_at, subs_updated_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            `, [
              metrics.account_id,
              metrics.date.value,
              parseFloat(metrics.total_spend) || 0,
              parseInt(metrics.total_texts_delivered) || 0,
              parseInt(metrics.coupons_redeemed) || 0,
              parseInt(metrics.active_subs_cnt) || 0,
              new Date().toISOString(),
              new Date().toISOString(),
              new Date().toISOString(),
              new Date().toISOString()
            ]);
            createdCount++;
          }
        }

        if (metricsData.length > chunkSize) {
          console.log(`   📈 Processed ${Math.min(i + chunkSize, metricsData.length)}/${metricsData.length} records...`);
        }
      }

      await client.query('COMMIT');

      console.log(`✅ Batch upsert for ${month}: ${updatedCount} updated, ${createdCount} created`);
      return { updatedCount, createdCount, totalProcessed: metricsData.length };

    } catch (error) {
      await client.query('ROLLBACK');
      console.error(`❌ Batch upsert failed for ${month}:`, error);
      throw error;
    } finally {
      client.release();
    }
  }

  async backfillMonth(month) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`🔄 Starting optimized backfill for ${month}`);
    console.log('='.repeat(60));

    const metricsData = await this.getAllDailyMetricsForMonth(month);
    const result = await this.batchUpsertMetrics(month, metricsData);

    console.log(`\n✅ ${month} backfill complete!`);
    console.log(`   Total records: ${result.totalProcessed}`);
    console.log(`   Updated: ${result.updatedCount}`);
    console.log(`   Created: ${result.createdCount}`);

    return result;
  }

  async close() {
    await this.pool.end();
    console.log('🔌 PostgreSQL connection pool closed');
  }
}

// CLI
if (import.meta.url === `file://${process.argv[1]}`) {
  const month = process.argv[2];

  if (!month || !/^\d{4}-\d{2}$/.test(month)) {
    console.error(`❌ Usage: node backfill-month-optimized.js YYYY-MM

Example:
  node backfill-month-optimized.js 2024-09
`);
    process.exit(1);
  }

  const backfill = new MonthlyBackfillOptimized();
  backfill.backfillMonth(month)
    .then(() => {
      console.log(`\n🎉 Backfill completed successfully!`);
      return backfill.close();
    })
    .then(() => {
      process.exit(0);
    })
    .catch(error => {
      console.error('\n❌ Backfill failed:', error);
      backfill.close().finally(() => process.exit(1));
    });
}

export { MonthlyBackfillOptimized };
