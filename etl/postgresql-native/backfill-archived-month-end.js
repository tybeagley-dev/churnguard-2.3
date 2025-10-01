import { BigQuery } from '@google-cloud/bigquery';
import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

class ArchivedMonthEndBackfill {
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
      console.error('❌ No BigQuery credentials found');
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
      statement_timeout: 300000,
      query_timeout: 300000,
      keepAlive: true,
      keepAliveInitialDelayMillis: 10000
    });

    this.pool.on('error', (err) => {
      console.error('❌ PostgreSQL pool error:', err);
    });
  }

  // Get archived accounts that missed their month-end MSA
  async getAffectedAccounts() {
    console.log('🔍 Identifying archived accounts missing month-end MSA data...');

    const query = `
      SELECT
        account_id,
        COALESCE(archived_at, earliest_unit_archived_at) as effective_archive_date,
        DATE_TRUNC('month', COALESCE(archived_at, earliest_unit_archived_at)::timestamp)::date as archive_month,
        (DATE_TRUNC('month', COALESCE(archived_at, earliest_unit_archived_at)::timestamp) + INTERVAL '1 month - 1 day')::date as last_day_of_archive_month
      FROM accounts
      WHERE status = 'ARCHIVED'
        AND COALESCE(archived_at, earliest_unit_archived_at) IS NOT NULL
        AND COALESCE(archived_at, earliest_unit_archived_at)::timestamp < (DATE_TRUNC('month', COALESCE(archived_at, earliest_unit_archived_at)::timestamp) + INTERVAL '1 month')::date
        AND COALESCE(archived_at, earliest_unit_archived_at)::date >= '2024-10-01'
      ORDER BY effective_archive_date
    `;

    const client = await this.pool.connect();
    try {
      const result = await client.query(query);
      console.log(`✅ Found ${result.rows.length} archived accounts that may be missing month-end MSA data`);
      return result.rows;
    } finally {
      client.release();
    }
  }

  // Get daily metrics data from BigQuery for a specific date
  async getMonthEndMetrics(accountId, date) {
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
        WHERE a.id = '${accountId}'
          AND (
            -- Include accounts that have launched on or before the processing date
            (a.launched_at IS NOT NULL AND DATE(a.launched_at) <= DATE('${date}'))
            OR
            -- Include accounts launched within the same month (captures pre-launch platform fees on 1st of month)
            (a.launched_at IS NOT NULL AND FORMAT_DATE('%Y-%m', DATE(a.launched_at)) = FORMAT_DATE('%Y-%m', DATE('${date}')))
            OR
            -- Include accounts with NULL launch date that have revenue activity
            (a.launched_at IS NULL)
          )
          AND (
            -- Account is not ARCHIVED status (include regardless of earliest_unit_archived_at)
            a.status != 'ARCHIVED'
            OR
            -- Account IS ARCHIVED and the date we're processing is BEFORE the archive date
            -- Use archived_at if it exists, otherwise fall back to earliest_unit_archived_at
            (a.status = 'ARCHIVED'
             AND COALESCE(a.archived_at, aad.earliest_unit_archived_at) IS NOT NULL
             AND DATE('${date}') < DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)))
            OR
            -- SPECIAL CASE: Include month-end date for the month of archiving
            -- MSA applies on last day of month even for accounts archived mid-month
            (a.status = 'ARCHIVED'
             AND COALESCE(a.archived_at, aad.earliest_unit_archived_at) IS NOT NULL
             AND FORMAT_DATE('%Y-%m', DATE('${date}')) = FORMAT_DATE('%Y-%m', DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)))
             AND EXTRACT(DAY FROM LAST_DAY(DATE('${date}'))) = EXTRACT(DAY FROM DATE('${date}')))
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

      -- Text message data
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

      -- Coupon data
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

      -- Subscription data
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

      -- Combine all metrics
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
        -- Include all accounts with valid launch dates (even with $0 revenue)
        ea.launched_at IS NOT NULL
        OR
        -- Include NULL launch date accounts ONLY if they have revenue
        (ea.launched_at IS NULL AND COALESCE(s.total_spend, 0) > 0)
      )
      ORDER BY account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    return rows;
  }

  // Upsert metrics to PostgreSQL
  async upsertMetrics(accountId, date, metrics) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      // Try UPDATE first, then INSERT
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
        accountId,
        date
      ]);

      if (updateResult.rowCount > 0) {
        await client.query('COMMIT');
        return 'updated';
      } else {
        // Insert if no existing row
        await client.query(`
          INSERT INTO daily_metrics (
            account_id, date, total_spend, total_texts_delivered,
            coupons_redeemed, active_subs_cnt, spend_updated_at,
            texts_updated_at, coupons_updated_at, subs_updated_at
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        `, [
          accountId,
          date,
          parseFloat(metrics.total_spend) || 0,
          parseInt(metrics.total_texts_delivered) || 0,
          parseInt(metrics.coupons_redeemed) || 0,
          parseInt(metrics.active_subs_cnt) || 0,
          new Date().toISOString(),
          new Date().toISOString(),
          new Date().toISOString(),
          new Date().toISOString()
        ]);
        await client.query('COMMIT');
        return 'created';
      }

    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  // Main backfill process
  async run() {
    console.log('🚀 Starting archived accounts month-end MSA backfill...\n');

    const affectedAccounts = await this.getAffectedAccounts();

    if (affectedAccounts.length === 0) {
      console.log('✅ No affected accounts found. Backfill complete.');
      return;
    }

    let processedCount = 0;
    let updatedCount = 0;
    let createdCount = 0;
    let noDataCount = 0;

    for (const account of affectedAccounts) {
      const { account_id, effective_archive_date, last_day_of_archive_month } = account;

      try {
        // Convert date to YYYY-MM-DD string format for BigQuery
        const monthEndDate = last_day_of_archive_month instanceof Date
          ? last_day_of_archive_month.toISOString().split('T')[0]
          : last_day_of_archive_month;

        console.log(`📊 Processing ${account_id} - Archive: ${effective_archive_date}, Month-end: ${monthEndDate}`);

        const metricsData = await this.getMonthEndMetrics(account_id, monthEndDate);

        if (metricsData.length === 0) {
          console.log(`   ⚠️  No data found for ${monthEndDate}`);
          noDataCount++;
        } else {
          const metrics = metricsData[0];
          const action = await this.upsertMetrics(account_id, monthEndDate, metrics);

          if (action === 'updated') {
            updatedCount++;
            console.log(`   ✅ Updated: $${metrics.total_spend}`);
          } else {
            createdCount++;
            console.log(`   ✅ Created: $${metrics.total_spend}`);
          }
        }

        processedCount++;

        // Progress indicator
        if (processedCount % 10 === 0) {
          console.log(`\n📈 Progress: ${processedCount}/${affectedAccounts.length} accounts processed\n`);
        }

      } catch (error) {
        console.error(`❌ Error processing ${account_id}:`, error.message);
      }
    }

    console.log('\n🎉 Archived accounts month-end backfill complete!');
    console.log(`📊 Summary:`);
    console.log(`   Accounts processed: ${processedCount}`);
    console.log(`   Records updated: ${updatedCount}`);
    console.log(`   Records created: ${createdCount}`);
    console.log(`   No data found: ${noDataCount}`);
  }

  // Clean shutdown
  async close() {
    await this.pool.end();
    console.log('🔌 PostgreSQL connection pool closed');
  }
}

// CLI execution
if (import.meta.url === `file://${process.argv[1]}`) {
  const backfill = new ArchivedMonthEndBackfill();

  backfill.run()
    .then(() => {
      console.log('🎯 Backfill completed successfully!');
      return backfill.close();
    })
    .then(() => {
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Backfill failed:', error);
      backfill.close().finally(() => process.exit(1));
    });
}

export { ArchivedMonthEndBackfill };