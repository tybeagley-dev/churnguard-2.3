import { BigQuery } from '@google-cloud/bigquery';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

class DailyTextsETLSQLite {
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

  async getTextsDataForDate(date) {
    console.log(`📱 Fetching texts data for ${date}...`);

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
        COUNT(DISTINCT t.id) as total_texts_delivered
      FROM public.texts t
      JOIN units.units u ON u.id = t.unit_id
      INNER JOIN accounts.accounts a ON u.account_id = a.id
      LEFT JOIN account_unit_archive_dates aad ON a.id = aad.account_id
      WHERE t.direction = 'OUTGOING'
        AND t.status = 'DELIVERED'
        AND DATE(t.created_at) = DATE('${date}')
        AND a.launched_at IS NOT NULL
        AND DATE(a.launched_at) <= DATE('${date}')
        AND (
          COALESCE(a.archived_at, aad.earliest_unit_archived_at) IS NULL
          OR DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)) > LAST_DAY(DATE('${date}'))
        )
      GROUP BY u.account_id
      HAVING total_texts_delivered > 0
      ORDER BY u.account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found texts data for ${rows.length} eligible accounts on ${date}`);
    return rows;
  }

  async updateMetricsTable(date, textsData) {
    const db = await this.getDatabase();
    
    console.log(`📊 Updating texts data for ${date}...`);
    
    let updatedCount = 0;
    let createdCount = 0;
    
    await db.exec('BEGIN TRANSACTION');
    
    for (const texts of textsData) {
      // Try to update existing row first
      const updateResult = await db.run(`
        UPDATE daily_metrics 
        SET total_texts_delivered = ?, texts_updated_at = ?
        WHERE account_id = ? AND date = ?
      `, [parseInt(texts.total_texts_delivered), new Date().toISOString(), texts.account_id, date]);
      
      if (updateResult.changes > 0) {
        updatedCount++;
      } else {
        // Insert new row if no existing row found
        await db.run(`
          INSERT INTO daily_metrics (
            account_id, date, total_spend, total_texts_delivered, 
            coupons_redeemed, active_subs_cnt, spend_updated_at,
            texts_updated_at, coupons_updated_at, subs_updated_at
          ) VALUES (?, ?, 0, ?, 0, 0, '', ?, '', '')
        `, [texts.account_id, date, parseInt(texts.total_texts_delivered), new Date().toISOString()]);
        createdCount++;
      }
    }
    
    await db.exec('COMMIT');
    await db.close();
    
    console.log(`✅ Texts ETL for ${date}: ${updatedCount} updated, ${createdCount} created`);
    return { updatedCount, createdCount };
  }

  async processDate(date) {
    const textsData = await this.getTextsDataForDate(date);
    const result = await this.updateMetricsTable(date, textsData);
    return result;
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const date = process.argv[2];
  if (!date) {
    console.error('❌ Usage: node daily-texts-etl-sqlite.js YYYY-MM-DD');
    process.exit(1);
  }

  const etl = new DailyTextsETLSQLite();
  etl.processDate(date)
    .then(result => {
      console.log(`🎉 Texts ETL completed for ${date}!`);
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Texts ETL failed:', error);
      process.exit(1);
    });
}

export { DailyTextsETLSQLite };