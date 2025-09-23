import { BigQuery } from '@google-cloud/bigquery';
import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

class DailySubsETLPostgreSQL {
  constructor() {
    this.bigquery = new BigQuery({
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
    });

    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
    });
  }

  async getDatabase() {
    return this.pool;
  }

  async getSubsDataForDate(date) {
    console.log(`👥 Fetching subscriber data for ${date}...`);

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
        COUNT(DISTINCT s.id) as active_subs_cnt
      FROM public.subscriptions s
      JOIN units.units u ON s.channel_id = u.id
      INNER JOIN accounts.accounts a ON u.account_id = a.id
      LEFT JOIN account_unit_archive_dates aad ON a.id = aad.account_id
      WHERE DATE(s.created_at) <= DATE('${date}')
        AND (s.deactivated_at IS NULL OR DATE(s.deactivated_at) > DATE('${date}'))
        AND a.launched_at IS NOT NULL
        AND DATE(a.launched_at) <= DATE('${date}')
        AND (
          COALESCE(a.archived_at, aad.earliest_unit_archived_at) IS NULL
          OR DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)) > LAST_DAY(DATE('${date}'))
        )
      GROUP BY u.account_id
      HAVING active_subs_cnt > 0
      ORDER BY u.account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found subscriber data for ${rows.length} eligible accounts on ${date}`);
    return rows;
  }

  async updateMetricsTable(date, subsData) {
    const db = await this.getDatabase();

    console.log(`📊 Updating subscriber data for ${date}...`);

    let updatedCount = 0;
    let createdCount = 0;

    await db.query('BEGIN');

    for (const subs of subsData) {
      // Try to update existing row first
      const updateResult = await db.query(`
        UPDATE daily_metrics
        SET active_subs_cnt = $1, subs_updated_at = $2
        WHERE account_id = $3 AND date = $4
      `, [parseInt(subs.active_subs_cnt), new Date().toISOString(), subs.account_id, date]);

      if (updateResult.rowCount > 0) {
        updatedCount++;
      } else {
        // Insert new row if no existing row found
        await db.query(`
          INSERT INTO daily_metrics (
            account_id, date, total_spend, total_texts_delivered,
            coupons_redeemed, active_subs_cnt, spend_updated_at,
            texts_updated_at, coupons_updated_at, subs_updated_at
          ) VALUES ($1, $2, 0, 0, 0, $3, '', '', '', $4)
        `, [subs.account_id, date, parseInt(subs.active_subs_cnt), new Date().toISOString()]);
        createdCount++;
      }
    }

    await db.query('COMMIT');

    console.log(`✅ Subs ETL for ${date}: ${updatedCount} updated, ${createdCount} created`);
    return { updatedCount, createdCount };
  }

  async processDate(date) {
    const subsData = await this.getSubsDataForDate(date);
    const result = await this.updateMetricsTable(date, subsData);
    return result;
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const date = process.argv[2];
  if (!date) {
    console.error('❌ Usage: node daily-subs-etl-postgresql.js YYYY-MM-DD');
    process.exit(1);
  }

  const etl = new DailySubsETLPostgreSQL();
  etl.processDate(date)
    .then(result => {
      console.log(`🎉 Subs ETL completed for ${date}!`);
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Subs ETL failed:', error);
      process.exit(1);
    });
}

export { DailySubsETLPostgreSQL };