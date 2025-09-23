import { BigQuery } from '@google-cloud/bigquery';
import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

class AccountsETLPostgreSQL {
  constructor() {
    // BigQuery client
    const bigqueryConfig = {
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
    };

    // Handle credentials: use JSON string in production, file path in development
    if (process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) {
      bigqueryConfig.credentials = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON);
    } else if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      bigqueryConfig.keyFilename = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    }

    this.bigquery = new BigQuery(bigqueryConfig);
  }

  async getDatabase() {
    return this.pool;
  }

  async setupTables() {
    console.log('🗄️  Setting up PostgreSQL tables...');
    const db = await this.getDatabase();

    // Create accounts table
    await db.query(`
      CREATE TABLE IF NOT EXISTS accounts (
        account_id TEXT PRIMARY KEY,
        account_name TEXT,
        status TEXT,
        launched_at TEXT,
        csm_owner TEXT,
        hubspot_id TEXT,
        archived_at TEXT,
        earliest_unit_archived_at TEXT,
        last_updated TEXT
      );
    `);

    // Create daily_metrics table with composite primary key
    await db.query(`
      CREATE TABLE IF NOT EXISTS daily_metrics (
        account_id TEXT,
        date TEXT,
        total_spend REAL DEFAULT 0,
        total_texts_delivered INTEGER DEFAULT 0,
        coupons_redeemed INTEGER DEFAULT 0,
        active_subs_cnt INTEGER DEFAULT 0,
        spend_updated_at TEXT,
        texts_updated_at TEXT,
        coupons_updated_at TEXT,
        subs_updated_at TEXT,
        PRIMARY KEY (account_id, date),
        FOREIGN KEY (account_id) REFERENCES accounts (account_id)
      );
    `);

    // Create monthly_metrics table
    await db.query(`
      CREATE TABLE IF NOT EXISTS monthly_metrics (
        account_id TEXT,
        month TEXT,
        month_label TEXT,
        total_spend REAL DEFAULT 0,
        total_texts_delivered INTEGER DEFAULT 0,
        total_coupons_redeemed INTEGER DEFAULT 0,
        avg_active_subs_cnt REAL DEFAULT 0,
        trending_risk_level TEXT,
        trending_risk_reasons TEXT,
        historical_risk_level TEXT,
        risk_reasons TEXT,
        created_at TEXT,
        updated_at TEXT,
        PRIMARY KEY (account_id, month),
        FOREIGN KEY (account_id) REFERENCES accounts (account_id)
      );
    `);

    // Create indexes for performance
    await db.query(`
      CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics(date);
    `);
    await db.query(`
      CREATE INDEX IF NOT EXISTS idx_daily_metrics_account ON daily_metrics(account_id);
    `);
    await db.query(`
      CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
    `);

    console.log('✅ PostgreSQL tables created successfully');
  }

  async getAccountsFromBigQuery() {
    console.log('🔍 Fetching accounts from BigQuery with historical range filtering...');

    // Include all accounts that were active during our simulation period
    const simulationStart = '2024-08-01';
    const simulationEnd = '2025-09-30';

    const query = `
      WITH account_unit_archive_dates AS (
        -- Get earliest unit archive dates for accounts (fallback archived_at logic from 2.1)
        SELECT
          u.account_id,
          MIN(u.archived_at) as earliest_unit_archived_at
        FROM units.units u
        WHERE u.status = 'ARCHIVED' AND u.archived_at IS NOT NULL
        GROUP BY u.account_id
      )

      SELECT
        a.id as account_id,
        a.name as account_name,
        a.status,
        a.launched_at,
        a.hubspot_id,
        a.archived_at,
        aad.earliest_unit_archived_at,
        COALESCE(CONCAT(o.first_name, ' ', o.last_name), 'Unassigned') as csm_owner
      FROM accounts.accounts a
      LEFT JOIN hubspot.companies comp ON a.hubspot_id = CAST(comp.hs_object_id AS STRING)
      LEFT JOIN hubspot.owners o ON o.id = comp.hubspot_owner_id
      LEFT JOIN account_unit_archive_dates aad ON a.id = aad.account_id
      WHERE
        -- Account was launched before or during simulation period
        (a.launched_at IS NOT NULL AND DATE(a.launched_at) <= DATE('${simulationEnd}'))
        AND (
          -- Either account is still active
          a.status != 'ARCHIVED'
          OR (
            -- Or account was archived after simulation start (include historical accounts)
            a.status = 'ARCHIVED'
            AND DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)) >= DATE('${simulationStart}')
          )
        )
        AND a.status IN ('LAUNCHED', 'PAUSED', 'FROZEN', 'ARCHIVED')
      ORDER BY a.name
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found ${rows.length} accounts (with historical range filtering ${simulationStart} to ${simulationEnd})`);
    return rows;
  }

  async populateAccounts() {
    await this.setupTables();

    const accounts = await this.getAccountsFromBigQuery();
    const db = await this.getDatabase();

    console.log('📝 Populating accounts table...');

    // Clear existing data
    await db.query('DELETE FROM accounts');

    // Insert all accounts
    for (const account of accounts) {
      // Handle dates safely - keep real dates or set to null
      let launchedAt = null;
      let archivedAt = null;
      let earliestUnitArchivedAt = null;

      if (account.launched_at && account.launched_at.value) {
        try {
          launchedAt = new Date(account.launched_at.value).toISOString().split('T')[0];
        } catch (e) {
          console.warn(`Invalid launched_at for ${account.account_id}:`, account.launched_at);
        }
      }

      if (account.archived_at && account.archived_at.value) {
        try {
          archivedAt = new Date(account.archived_at.value).toISOString().split('T')[0];
        } catch (e) {
          console.warn(`Invalid archived_at for ${account.account_id}:`, account.archived_at);
        }
      }

      if (account.earliest_unit_archived_at && account.earliest_unit_archived_at.value) {
        try {
          earliestUnitArchivedAt = new Date(account.earliest_unit_archived_at.value).toISOString().split('T')[0];
        } catch (e) {
          console.warn(`Invalid earliest_unit_archived_at for ${account.account_id}:`, account.earliest_unit_archived_at);
        }
      }

      await db.query(`
        INSERT INTO accounts (
          account_id, account_name, status, launched_at,
          csm_owner, hubspot_id, archived_at, earliest_unit_archived_at, last_updated
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `, [
        account.account_id,
        account.account_name,
        account.status,
        launchedAt,
        account.csm_owner,
        account.hubspot_id || null,
        archivedAt,
        earliestUnitArchivedAt,
        new Date().toISOString()
      ]);
    }

    console.log(`✅ Successfully populated ${accounts.length} accounts`);
    return accounts.length;
  }

  async getAccountCount() {
    const db = await this.getDatabase();
    const result = await db.query('SELECT COUNT(*) as count FROM accounts');
    return result.rows[0].count;
  }
}

export { AccountsETLPostgreSQL };