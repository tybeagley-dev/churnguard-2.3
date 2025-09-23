import { BigQuery } from '@google-cloud/bigquery';
import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

class DailySpendETLPostgreSQL {
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

    // Initialize PostgreSQL pool
    if (!process.env.DATABASE_URL) {
      throw new Error('DATABASE_URL environment variable is required');
    }

    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
    });
  }

  async getDatabase() {
    return this.pool;
  }

  async getSpendDataForDate(date) {
    console.log(`💰 Fetching spend data for ${date}...`);

    const query = `
      WITH account_unit_archive_dates AS (
        SELECT
          u.account_id,
          MIN(u.archived_at) as earliest_unit_archived_at
        FROM units.units u
        WHERE u.status = 'ARCHIVED' AND u.archived_at IS NOT NULL
        GROUP BY u.account_id
      )

      SELECT
        tr.account_id,
        SUM(tr.total) as total_spend
      FROM dbt_models.total_revenue_by_account_and_date tr
      INNER JOIN accounts.accounts a ON tr.account_id = a.id
      LEFT JOIN account_unit_archive_dates aad ON a.id = aad.account_id
      WHERE tr.date = DATE('${date}')
        AND a.launched_at IS NOT NULL
        AND DATE(a.launched_at) <= DATE('${date}')
        AND (
          COALESCE(a.archived_at, aad.earliest_unit_archived_at) IS NULL
          OR DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)) > LAST_DAY(DATE('${date}'))
        )
      GROUP BY tr.account_id
      HAVING total_spend > 0
      ORDER BY tr.account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found spend data for ${rows.length} eligible accounts on ${date}`);
    return rows;
  }

  async updateMetricsTable(date, spendData) {
    const db = await this.getDatabase();

    console.log(`📊 Updating spend data for ${date}...`);

    let updatedCount = 0;
    let createdCount = 0;

    await db.query('BEGIN');

    for (const spend of spendData) {
      // Try to update existing row first
      const updateResult = await db.query(`
        UPDATE daily_metrics
        SET total_spend = $1, spend_updated_at = $2
        WHERE account_id = $3 AND date = $4
      `, [parseFloat(spend.total_spend), new Date().toISOString(), spend.account_id, date]);

      if (updateResult.rowCount > 0) {
        updatedCount++;
      } else {
        // Insert new row if no existing row found
        await db.query(`
          INSERT INTO daily_metrics (
            account_id, date, total_spend, total_texts_delivered,
            coupons_redeemed, active_subs_cnt, spend_updated_at,
            texts_updated_at, coupons_updated_at, subs_updated_at
          ) VALUES ($1, $2, $3, 0, 0, 0, $4, '', '', '')
        `, [spend.account_id, date, parseFloat(spend.total_spend), new Date().toISOString()]);
        createdCount++;
      }
    }

    await db.query('COMMIT');

    console.log(`✅ Spend ETL for ${date}: ${updatedCount} updated, ${createdCount} created`);
    return { updatedCount, createdCount };
  }

  async processDate(date) {
    const spendData = await this.getSpendDataForDate(date);
    const result = await this.updateMetricsTable(date, spendData);
    return result;
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const date = process.argv[2];
  if (!date) {
    console.error('❌ Usage: node daily-spend-etl-postgresql.js YYYY-MM-DD');
    process.exit(1);
  }

  const etl = new DailySpendETLPostgreSQL();
  etl.processDate(date)
    .then(result => {
      console.log(`🎉 Spend ETL completed for ${date}!`);
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Spend ETL failed:', error);
      process.exit(1);
    });
}

export { DailySpendETLPostgreSQL };