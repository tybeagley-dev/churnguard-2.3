import { BigQuery } from '@google-cloud/bigquery';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
import fs from 'fs';

dotenv.config();

class DailySpendETL {
  constructor() {
    this.bigquery = new BigQuery({
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
    });

    const credentials = JSON.parse(fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS, 'utf8'));
    this.serviceAccountAuth = new JWT({
      email: credentials.client_email,
      key: credentials.private_key,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
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

  async updateMetricsSheet(date, spendData) {
    const doc = new GoogleSpreadsheet(process.env.GOOGLE_SHEETS_ID, this.serviceAccountAuth);
    await doc.loadInfo();
    
    const metricsSheet = doc.sheetsByTitle['daily_metrics'];
    if (!metricsSheet) {
      throw new Error('daily_metrics sheet not found. Run accounts-etl.js first.');
    }

    // Load existing rows
    await metricsSheet.loadCells();
    const rows = await metricsSheet.getRows();
    
    console.log(`📊 Updating spend data for ${date}...`);
    
    let updatedCount = 0;
    let createdCount = 0;
    
    for (const spend of spendData) {
      // Find existing row for this account + date
      let existingRow = rows.find(row => 
        row.get('account_id') === spend.account_id && 
        row.get('date') === date
      );
      
      if (existingRow) {
        // Update existing row
        existingRow.set('total_spend', parseFloat(spend.total_spend));
        existingRow.set('spend_updated_at', new Date().toISOString());
        await existingRow.save();
        updatedCount++;
      } else {
        // Create new row
        await metricsSheet.addRow({
          account_id: spend.account_id,
          date: date,
          total_spend: parseFloat(spend.total_spend),
          total_texts_delivered: 0,
          coupons_redeemed: 0,
          active_subs_cnt: 0,
          spend_updated_at: new Date().toISOString(),
          texts_updated_at: '',
          coupons_updated_at: '',
          subs_updated_at: ''
        });
        createdCount++;
      }
    }
    
    console.log(`✅ Spend ETL for ${date}: ${updatedCount} updated, ${createdCount} created`);
    return { updatedCount, createdCount };
  }

  async processDate(date) {
    const spendData = await this.getSpendDataForDate(date);
    const result = await this.updateMetricsSheet(date, spendData);
    return result;
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const date = process.argv[2];
  if (!date) {
    console.error('❌ Usage: node daily-spend-etl.js YYYY-MM-DD');
    process.exit(1);
  }

  const etl = new DailySpendETL();
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

export { DailySpendETL };