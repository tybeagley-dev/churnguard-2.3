import { BigQuery } from '@google-cloud/bigquery';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
import dotenv from 'dotenv';
import fs from 'fs';

dotenv.config();

class AccountsETL {
  constructor() {
    // BigQuery client - use existing credentials file
    this.bigquery = new BigQuery({
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
    });

    // Google Sheets client - use same credentials for both BigQuery and Sheets
    const credentials = JSON.parse(fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS, 'utf8'));
    this.serviceAccountAuth = new JWT({
      email: credentials.client_email,
      key: credentials.private_key,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
  }

  async getAccountsFromBigQuery() {
    console.log('🔍 Fetching accounts from BigQuery...');
    
    const query = `
      SELECT 
        a.id as account_id,
        a.name as account_name,
        a.status,
        a.launched_at,
        a.hubspot_id,
        a.archived_at,
        COALESCE(CONCAT(o.first_name, ' ', o.last_name), 'Unassigned') as csm_owner
      FROM accounts.accounts a
      LEFT JOIN hubspot.companies comp ON a.hubspot_id = CAST(comp.hs_object_id AS STRING)
      LEFT JOIN hubspot.owners o ON o.id = comp.hubspot_owner_id
      WHERE a.launched_at IS NOT NULL
        AND a.status IN ('LAUNCHED', 'PAUSED', 'FROZEN', 'ARCHIVED')
      ORDER BY a.name
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found ${rows.length} accounts`);
    return rows;
  }

  async setupGoogleSheet() {
    console.log('📊 Setting up Google Sheets...');
    
    const doc = new GoogleSpreadsheet(process.env.GOOGLE_SHEETS_ID, this.serviceAccountAuth);
    await doc.loadInfo();
    
    // Create or get accounts sheet
    let accountsSheet;
    try {
      accountsSheet = doc.sheetsByTitle['accounts'];
      if (!accountsSheet) {
        accountsSheet = await doc.addSheet({ 
          title: 'accounts',
          headerValues: ['account_id', 'account_name', 'status', 'launched_at', 'csm_owner', 'hubspot_id', 'archived_at', 'last_updated']
        });
      }
    } catch (error) {
      accountsSheet = await doc.addSheet({ 
        title: 'accounts',
        headerValues: ['account_id', 'account_name', 'status', 'launched_at', 'csm_owner', 'hubspot_id', 'archived_at', 'last_updated']
      });
    }

    // Create or get daily_metrics sheet
    let metricsSheet;
    try {
      metricsSheet = doc.sheetsByTitle['daily_metrics'];
      if (!metricsSheet) {
        metricsSheet = await doc.addSheet({ 
          title: 'daily_metrics',
          headerValues: ['account_id', 'date', 'total_spend', 'total_texts_delivered', 'coupons_redeemed', 'active_subs_cnt', 'spend_updated_at', 'texts_updated_at', 'coupons_updated_at', 'subs_updated_at']
        });
      }
    } catch (error) {
      metricsSheet = await doc.addSheet({ 
        title: 'daily_metrics',
        headerValues: ['account_id', 'date', 'total_spend', 'total_texts_delivered', 'coupons_redeemed', 'active_subs_cnt', 'spend_updated_at', 'texts_updated_at', 'coupons_updated_at', 'subs_updated_at']
      });
    }

    return { doc, accountsSheet, metricsSheet };
  }

  async populateAccounts() {
    const accounts = await this.getAccountsFromBigQuery();
    const { accountsSheet } = await this.setupGoogleSheet();

    console.log('📝 Populating accounts sheet...');
    
    // Clear existing data (keep headers)
    await accountsSheet.clear();
    await accountsSheet.setHeaderRow(['account_id', 'account_name', 'status', 'launched_at', 'csm_owner', 'hubspot_id', 'archived_at', 'last_updated']);

    // Prepare rows for batch insert
    const rows = accounts.map(account => ({
      account_id: account.account_id,
      account_name: account.account_name,
      status: account.status,
      launched_at: account.launched_at ? new Date(account.launched_at).toISOString().split('T')[0] : '',
      csm_owner: account.csm_owner,
      hubspot_id: account.hubspot_id || '',
      archived_at: account.archived_at ? new Date(account.archived_at).toISOString().split('T')[0] : '',
      last_updated: new Date().toISOString()
    }));

    // Batch insert
    await accountsSheet.addRows(rows);
    
    console.log(`✅ Successfully populated ${rows.length} accounts`);
    return rows.length;
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const etl = new AccountsETL();
  etl.populateAccounts()
    .then(count => {
      console.log(`🎉 Accounts ETL completed! Populated ${count} accounts.`);
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Accounts ETL failed:', error);
      process.exit(1);
    });
}

export { AccountsETL };