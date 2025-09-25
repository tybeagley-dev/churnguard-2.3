import { BigQuery } from '@google-cloud/bigquery';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

class AccountsETLSQLite {
  constructor() {
    // BigQuery client - use existing credentials file
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

  async setupTables() {
    console.log('🗄️  Setting up database tables...');
    const db = await this.getDatabase();

    // Check if this is PostgreSQL (has query method) vs SQLite (has exec method)
    const isPostgreSQL = typeof db.query === 'function' && !db.exec;

    if (isPostgreSQL) {
      // PostgreSQL table creation
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
    } else {
      // SQLite table creation
      await db.exec(`
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
    }

    // Create daily_metrics table with composite primary key
    await db.exec(`
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

    // Create indexes for performance
    await db.exec(`
      CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics(date);
      CREATE INDEX IF NOT EXISTS idx_daily_metrics_account ON daily_metrics(account_id);
      CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
    `);

    await db.close();
    console.log('✅ SQLite tables created successfully');
  }

  async getAccountsFromBigQuery() {
    console.log('🔍 Fetching accounts from BigQuery with historical range filtering...');
    
    // Include all accounts that were active during our simulation period (Aug 2024 - Sep 2025)
    const simulationStart = '2024-08-01';  // Start of simulation
    const simulationEnd = '2025-09-30';   // End of simulation
    
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
    await db.exec('DELETE FROM accounts');

    // Prepare insert statement
    const insertStmt = await db.prepare(`
      INSERT INTO accounts (
        account_id, account_name, status, launched_at, 
        csm_owner, hubspot_id, archived_at, earliest_unit_archived_at, last_updated
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    // Insert all accounts in a transaction for speed
    await db.exec('BEGIN TRANSACTION');
    
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
      
      await insertStmt.run(
        account.account_id,
        account.account_name,
        account.status,
        launchedAt,
        account.csm_owner,
        account.hubspot_id || null,
        archivedAt,
        earliestUnitArchivedAt,
        new Date().toISOString()
      );
    }
    
    await db.exec('COMMIT');
    await insertStmt.finalize();
    await db.close();
    
    console.log(`✅ Successfully populated ${accounts.length} accounts`);
    return accounts.length;
  }

  async getAccountCount() {
    const db = await this.getDatabase();
    const result = await db.get('SELECT COUNT(*) as count FROM accounts');
    await db.close();
    return result.count;
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const etl = new AccountsETLSQLite();
  etl.populateAccounts()
    .then(count => {
      console.log(`🎉 Accounts ETL completed! Populated ${count} accounts.`);
      console.log(`📊 Database location: ${process.env.SQLITE_DB_PATH || './data/churnguard_simulation.db'}`);
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Accounts ETL failed:', error);
      process.exit(1);
    });
}

export { AccountsETLSQLite };