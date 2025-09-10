import { BigQuery } from '@google-cloud/bigquery';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

class DailyCouponsETLSQLite {
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

  async getCouponsDataForDate(date) {
    console.log(`🎫 Fetching coupons data for ${date}...`);
    
    const query = `
      SELECT 
        u.account_id,
        COUNT(DISTINCT c.id) as coupons_redeemed
      FROM promos.coupons c
      JOIN units.units u ON u.id = c.unit_id
      WHERE c.is_redeemed = TRUE
        AND DATE(c.redeemed_at) = DATE('${date}')
      GROUP BY u.account_id
      HAVING coupons_redeemed > 0
      ORDER BY u.account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found coupons data for ${rows.length} accounts on ${date}`);
    return rows;
  }

  async updateMetricsTable(date, couponsData) {
    const db = await this.getDatabase();
    
    console.log(`📊 Updating coupons data for ${date}...`);
    
    let updatedCount = 0;
    let createdCount = 0;
    
    await db.exec('BEGIN TRANSACTION');
    
    for (const coupons of couponsData) {
      // Try to update existing row first
      const updateResult = await db.run(`
        UPDATE daily_metrics 
        SET coupons_redeemed = ?, coupons_updated_at = ?
        WHERE account_id = ? AND date = ?
      `, [parseInt(coupons.coupons_redeemed), new Date().toISOString(), coupons.account_id, date]);
      
      if (updateResult.changes > 0) {
        updatedCount++;
      } else {
        // Insert new row if no existing row found
        await db.run(`
          INSERT INTO daily_metrics (
            account_id, date, total_spend, total_texts_delivered, 
            coupons_redeemed, active_subs_cnt, spend_updated_at,
            texts_updated_at, coupons_updated_at, subs_updated_at
          ) VALUES (?, ?, 0, 0, ?, 0, '', '', ?, '')
        `, [coupons.account_id, date, parseInt(coupons.coupons_redeemed), new Date().toISOString()]);
        createdCount++;
      }
    }
    
    await db.exec('COMMIT');
    await db.close();
    
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
    console.error('❌ Usage: node daily-coupons-etl-sqlite.js YYYY-MM-DD');
    process.exit(1);
  }

  const etl = new DailyCouponsETLSQLite();
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

export { DailyCouponsETLSQLite };