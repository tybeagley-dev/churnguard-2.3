import { BigQuery } from '@google-cloud/bigquery';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
import fs from 'fs';

dotenv.config();

class DailySubsETL {
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

  async getSubsDataForDate(date) {
    console.log(`👥 Fetching subscriber data for ${date}...`);
    
    // Get active subscribers as of the given date
    const query = `
      SELECT 
        u.account_id,
        COUNT(DISTINCT s.id) as active_subs_cnt
      FROM public.subscriptions s
      JOIN units.units u ON s.channel_id = u.id
      WHERE DATE(s.created_at) <= DATE('${date}')
        AND (s.deactivated_at IS NULL OR DATE(s.deactivated_at) > DATE('${date}'))
      GROUP BY u.account_id
      HAVING active_subs_cnt > 0
      ORDER BY u.account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found subscriber data for ${rows.length} accounts on ${date}`);
    return rows;
  }

  async updateMetricsSheet(date, subsData) {
    const doc = new GoogleSpreadsheet(process.env.GOOGLE_SHEETS_ID, this.serviceAccountAuth);
    await doc.loadInfo();
    
    const metricsSheet = doc.sheetsByTitle['daily_metrics'];
    if (!metricsSheet) {
      throw new Error('daily_metrics sheet not found. Run accounts-etl.js first.');
    }

    const rows = await metricsSheet.getRows();
    
    console.log(`📊 Updating subscriber data for ${date}...`);
    
    let updatedCount = 0;
    let createdCount = 0;
    
    for (const subs of subsData) {
      // Find existing row for this account + date
      let existingRow = rows.find(row => 
        row.get('account_id') === subs.account_id && 
        row.get('date') === date
      );
      
      if (existingRow) {
        // Update existing row
        existingRow.set('active_subs_cnt', parseInt(subs.active_subs_cnt));
        existingRow.set('subs_updated_at', new Date().toISOString());
        await existingRow.save();
        updatedCount++;
      } else {
        // Create new row
        await metricsSheet.addRow({
          account_id: subs.account_id,
          date: date,
          total_spend: 0,
          total_texts_delivered: 0,
          coupons_redeemed: 0,
          active_subs_cnt: parseInt(subs.active_subs_cnt),
          spend_updated_at: '',
          texts_updated_at: '',
          coupons_updated_at: '',
          subs_updated_at: new Date().toISOString()
        });
        createdCount++;
      }
    }
    
    console.log(`✅ Subs ETL for ${date}: ${updatedCount} updated, ${createdCount} created`);
    return { updatedCount, createdCount };
  }

  async processDate(date) {
    const subsData = await this.getSubsDataForDate(date);
    const result = await this.updateMetricsSheet(date, subsData);
    return result;
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const date = process.argv[2];
  if (!date) {
    console.error('❌ Usage: node daily-subs-etl.js YYYY-MM-DD');
    process.exit(1);
  }

  const etl = new DailySubsETL();
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

export { DailySubsETL };