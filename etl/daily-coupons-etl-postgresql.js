import { BigQuery } from '@google-cloud/bigquery';
import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

class DailyCouponsETLPostgreSQL {
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

  async getCouponsDataForDate(date) {
    console.log(`🎫 Fetching coupons data for ${date}...`);

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
        u.account_id,
        COUNT(DISTINCT c.id) as coupons_redeemed
      FROM promos.coupons c
      JOIN units.units u ON u.id = c.unit_id
      INNER JOIN accounts.accounts a ON u.account_id = a.id
      LEFT JOIN account_unit_archive_dates aad ON a.id = aad.account_id
      WHERE c.is_redeemed = TRUE
        AND DATE(c.redeemed_at) = DATE('${date}')
        AND a.launched_at IS NOT NULL
        AND DATE(a.launched_at) <= DATE('${date}')
        AND (
          COALESCE(a.archived_at, aad.earliest_unit_archived_at) IS NULL
          OR DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)) > LAST_DAY(DATE('${date}'))
        )
      GROUP BY u.account_id
      HAVING coupons_redeemed > 0
      ORDER BY u.account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found coupons data for ${rows.length} eligible accounts on ${date}`);
    return rows;
  }

  async updateMetricsTable(date, couponsData) {
    const db = await this.getDatabase();

    console.log(`📊 Updating coupons data for ${date}...`);

    let updatedCount = 0;
    let createdCount = 0;

    await db.query('BEGIN');

    for (const coupons of couponsData) {
      // Try to update existing row first
      const updateResult = await db.query(`
        UPDATE daily_metrics
        SET coupons_redeemed = $1, coupons_updated_at = $2
        WHERE account_id = $3 AND date = $4
      `, [parseInt(coupons.coupons_redeemed), new Date().toISOString(), coupons.account_id, date]);

      if (updateResult.rowCount > 0) {
        updatedCount++;
      } else {
        // Insert new row if no existing row found
        await db.query(`
          INSERT INTO daily_metrics (
            account_id, date, total_spend, total_texts_delivered,
            coupons_redeemed, active_subs_cnt, spend_updated_at,
            texts_updated_at, coupons_updated_at, subs_updated_at
          ) VALUES ($1, $2, 0, 0, $3, 0, '', '', $4, '')
        `, [coupons.account_id, date, parseInt(coupons.coupons_redeemed), new Date().toISOString()]);
        createdCount++;
      }
    }

    await db.query('COMMIT');

    console.log(`✅ Coupons ETL for ${date}: ${updatedCount} updated, ${createdCount} created`);
    return { updatedCount, createdCount };
  }

  async processDate(date) {
    const couponsData = await this.getCouponsDataForDate(date);
    const result = await this.updateMetricsTable(date, couponsData);
    return result;
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const date = process.argv[2];
  if (!date) {
    console.error('❌ Usage: node daily-coupons-etl-postgresql.js YYYY-MM-DD');
    process.exit(1);
  }

  const etl = new DailyCouponsETLPostgreSQL();
  etl.processDate(date)
    .then(result => {
      console.log(`🎉 Coupons ETL completed for ${date}!`);
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Coupons ETL failed:', error);
      process.exit(1);
    });
}

export { DailyCouponsETLPostgreSQL };