import { BigQuery } from '@google-cloud/bigquery';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

class DailySubsETLSQLite {
  constructor() {
    this.bigquery = new BigQuery({
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
    });
    this.dbPath = process.env.SQLITE_DB_PATH || './data/churnguard_simulation.db';
  }

  async getDatabase() {
    const db = await open({
      filename: this.dbPath,
      driver: sqlite3.Database
    });
    return db;
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
    
    await db.exec('BEGIN TRANSACTION');
    
    for (const subs of subsData) {
      // Try to update existing row first
      const updateResult = await db.run(`
        UPDATE daily_metrics 
        SET active_subs_cnt = ?, subs_updated_at = ?
        WHERE account_id = ? AND date = ?
      `, [parseInt(subs.active_subs_cnt), new Date().toISOString(), subs.account_id, date]);
      
      if (updateResult.changes > 0) {
        updatedCount++;
      } else {
        // Insert new row if no existing row found
        await db.run(`
          INSERT INTO daily_metrics (
            account_id, date, total_spend, total_texts_delivered, 
            coupons_redeemed, active_subs_cnt, spend_updated_at,
            texts_updated_at, coupons_updated_at, subs_updated_at
          ) VALUES (?, ?, 0, 0, 0, ?, '', '', '', ?)
        `, [subs.account_id, date, parseInt(subs.active_subs_cnt), new Date().toISOString()]);
        createdCount++;
      }
    }
    
    await db.exec('COMMIT');
    await db.close();
    
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
    console.error('❌ Usage: node daily-subs-etl-sqlite.js YYYY-MM-DD');
    process.exit(1);
  }

  const etl = new DailySubsETLSQLite();
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

export { DailySubsETLSQLite };