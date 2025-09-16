import { BigQuery } from '@google-cloud/bigquery';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
import fs from 'fs';

dotenv.config();

class DailyTextsETL {
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

  async getTextsDataForDate(date) {
    console.log(`📱 Fetching texts data for ${date}...`);
    
    const query = `
      SELECT 
        u.account_id,
        COUNT(DISTINCT t.id) as total_texts_delivered
      FROM public.texts t
      JOIN units.units u ON u.id = t.unit_id
      WHERE t.direction = 'OUTGOING' 
        AND t.status = 'DELIVERED'
        AND DATE(t.created_at) = DATE('${date}')
      GROUP BY u.account_id
      HAVING total_texts_delivered > 0
      ORDER BY u.account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found texts data for ${rows.length} accounts on ${date}`);
    return rows;
  }

  async updateMetricsSheet(date, textsData) {
    const doc = new GoogleSpreadsheet(process.env.GOOGLE_SHEETS_ID, this.serviceAccountAuth);
    await doc.loadInfo();
    
    const metricsSheet = doc.sheetsByTitle['daily_metrics'];
    if (!metricsSheet) {
      throw new Error('daily_metrics sheet not found. Run accounts-etl.js first.');
    }

    const rows = await metricsSheet.getRows();
    
    console.log(`📊 Updating texts data for ${date}...`);
    
    let updatedCount = 0;
    let createdCount = 0;
    
    for (const texts of textsData) {
      // Find existing row for this account + date
      let existingRow = rows.find(row => 
        row.get('account_id') === texts.account_id && 
        row.get('date') === date
      );
      
      if (existingRow) {
        // Update existing row
        existingRow.set('total_texts_delivered', parseInt(texts.total_texts_delivered));
        existingRow.set('texts_updated_at', new Date().toISOString());
        await existingRow.save();
        updatedCount++;
      } else {
        // Create new row
        await metricsSheet.addRow({
          account_id: texts.account_id,
          date: date,
          total_spend: 0,
          total_texts_delivered: parseInt(texts.total_texts_delivered),
          coupons_redeemed: 0,
          active_subs_cnt: 0,
          spend_updated_at: '',
          texts_updated_at: new Date().toISOString(),
          coupons_updated_at: '',
          subs_updated_at: ''
        });
        createdCount++;
      }
    }
    
    console.log(`✅ Texts ETL for ${date}: ${updatedCount} updated, ${createdCount} created`);
    return { updatedCount, createdCount };
  }

  async processDate(date) {
    const textsData = await this.getTextsDataForDate(date);
    const result = await this.updateMetricsSheet(date, textsData);
    return result;
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const date = process.argv[2];
  if (!date) {
    console.error('❌ Usage: node daily-texts-etl.js YYYY-MM-DD');
    process.exit(1);
  }

  const etl = new DailyTextsETL();
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

export { DailyTextsETL };