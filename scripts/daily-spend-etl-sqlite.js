import { BigQuery } from '@google-cloud/bigquery';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

class DailySpendETLSQLite {
  constructor() {
    this.bigquery = new BigQuery({
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
    });
    this.dbPath = process.env.SQLITE_DB_PATH || './churnguard_simulation.db';
  }

  async getDatabase() {
    const db = await open({
      filename: this.dbPath,
      driver: sqlite3.Database
    });
    return db;
  }

  async getSpendDataForDate(date) {
    console.log(`💰 Fetching spend data for ${date}...`);
    
    const query = `
      SELECT 
        account_id,
        SUM(total) as total_spend
      FROM dbt_models.total_revenue_by_account_and_date 
      WHERE date = DATE('${date}')
      GROUP BY account_id
      HAVING total_spend > 0
      ORDER BY account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found spend data for ${rows.length} accounts on ${date}`);
    return rows;
  }

  async updateMetricsTable(date, spendData) {
    const db = await this.getDatabase();
    
    console.log(`📊 Updating spend data for ${date}...`);
    
    let updatedCount = 0;
    let createdCount = 0;
    
    await db.exec('BEGIN TRANSACTION');
    
    for (const spend of spendData) {
      // Try to update existing row first
      const updateResult = await db.run(`
        UPDATE daily_metrics 
        SET total_spend = ?, spend_updated_at = ?
        WHERE account_id = ? AND date = ?
      `, [parseFloat(spend.total_spend), new Date().toISOString(), spend.account_id, date]);
      
      if (updateResult.changes > 0) {
        updatedCount++;
      } else {
        // Insert new row if no existing row found
        await db.run(`
          INSERT INTO daily_metrics (
            account_id, date, total_spend, total_texts_delivered, 
            coupons_redeemed, active_subs_cnt, spend_updated_at,
            texts_updated_at, coupons_updated_at, subs_updated_at
          ) VALUES (?, ?, ?, 0, 0, 0, ?, '', '', '')
        `, [spend.account_id, date, parseFloat(spend.total_spend), new Date().toISOString()]);
        createdCount++;
      }
    }
    
    await db.exec('COMMIT');
    await db.close();
    
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
    console.error('❌ Usage: node daily-spend-etl-sqlite.js YYYY-MM-DD');
    process.exit(1);
  }

  const etl = new DailySpendETLSQLite();
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

export { DailySpendETLSQLite };