import { BigQuery } from '@google-cloud/bigquery';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
import fs from 'fs';

dotenv.config();

class DailyCouponsETL {
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

  async updateMetricsSheet(date, couponsData) {
    const doc = new GoogleSpreadsheet(process.env.GOOGLE_SHEETS_ID, this.serviceAccountAuth);
    await doc.loadInfo();
    
    const metricsSheet = doc.sheetsByTitle['daily_metrics'];
    if (!metricsSheet) {
      throw new Error('daily_metrics sheet not found. Run accounts-etl.js first.');
    }

    const rows = await metricsSheet.getRows();
    
    console.log(`📊 Updating coupons data for ${date}...`);
    
    let updatedCount = 0;
    let createdCount = 0;
    
    for (const coupons of couponsData) {
      // Find existing row for this account + date
      let existingRow = rows.find(row => 
        row.get('account_id') === coupons.account_id && 
        row.get('date') === date
      );
      
      if (existingRow) {
        // Update existing row
        existingRow.set('coupons_redeemed', parseInt(coupons.coupons_redeemed));
        existingRow.set('coupons_updated_at', new Date().toISOString());
        await existingRow.save();
        updatedCount++;
      } else {
        // Create new row
        await metricsSheet.addRow({
          account_id: coupons.account_id,
          date: date,
          total_spend: 0,
          total_texts_delivered: 0,
          coupons_redeemed: parseInt(coupons.coupons_redeemed),
          active_subs_cnt: 0,
          spend_updated_at: '',
          texts_updated_at: '',
          coupons_updated_at: new Date().toISOString(),
          subs_updated_at: ''
        });
        createdCount++;
      }
    }
    
    console.log(`✅ Coupons ETL for ${date}: ${updatedCount} updated, ${createdCount} created`);
    return { updatedCount, createdCount };
  }

  async processDate(date) {
    const couponsData = await this.getCouponsDataForDate(date);
    const result = await this.updateMetricsSheet(date, couponsData);
    return result;
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const date = process.argv[2];
  if (!date) {
    console.error('❌ Usage: node daily-coupons-etl.js YYYY-MM-DD');
    process.exit(1);
  }

  const etl = new DailyCouponsETL();
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

export { DailyCouponsETL };