import { BigQuery } from '@google-cloud/bigquery';
import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

class AccountsETLPostgresNative {
  constructor() {
    // BigQuery client with proper credential handling
    const bigqueryConfig = {
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
    };

    // Handle credentials: use JSON string in production, file path in development
    if (process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) {
      try {
        bigqueryConfig.credentials = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON);
      } catch (error) {
        console.error('❌ Failed to parse GOOGLE_APPLICATION_CREDENTIALS_JSON:', error);
        throw new Error('Invalid BigQuery credentials JSON');
      }
    } else if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      bigqueryConfig.keyFilename = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    } else {
      console.error('❌ No BigQuery credentials found. Set GOOGLE_APPLICATION_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS');
      throw new Error('Missing BigQuery credentials');
    }

    this.bigquery = new BigQuery(bigqueryConfig);

    // Cost tracking configuration
    this.enableCostTracking = process.env.BIGQUERY_COST_TRACKING === 'true';
    this.costTrackingLevel = process.env.BIGQUERY_COST_TRACKING_LEVEL || 'summary'; // 'summary' or 'detailed'

    // Initialize PostgreSQL pool with proper SSL configuration
    if (!process.env.DATABASE_URL) {
      throw new Error('DATABASE_URL environment variable is required');
    }

    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: {
        rejectUnauthorized: false,
        sslmode: 'require'
      },
      max: 10,                    // Maximum connections in pool
      idleTimeoutMillis: 30000,   // Close idle connections after 30s
      connectionTimeoutMillis: 10000,  // Wait 10s for connection
      statement_timeout: 300000,  // 5 minute query timeout
      query_timeout: 300000,      // 5 minute query timeout
      keepAlive: true,
      keepAliveInitialDelayMillis: 10000
    });

    // Test connection on startup
    this.pool.on('error', (err) => {
      console.error('❌ PostgreSQL pool error:', err);
    });
  }

  async testConnection() {
    const client = await this.pool.connect();
    try {
      const result = await client.query('SELECT NOW() as current_time');
      console.log(`✅ PostgreSQL connected at ${result.rows[0].current_time}`);
    } catch (error) {
      console.error('❌ PostgreSQL connection test failed:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  async setupTables() {
    console.log('🗄️  Setting up database tables...');
    const client = await this.pool.connect();

    try {
      // PostgreSQL table creation with proper types
      await client.query(`
        CREATE TABLE IF NOT EXISTS accounts (
          account_id TEXT PRIMARY KEY,
          account_name TEXT,
          status TEXT,
          launched_at DATE,
          csm_owner TEXT,
          hubspot_id TEXT,
          archived_at DATE,
          earliest_unit_archived_at DATE,
          last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      // Create indexes for performance
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
        CREATE INDEX IF NOT EXISTS idx_accounts_launched_at ON accounts(launched_at);
        CREATE INDEX IF NOT EXISTS idx_accounts_hubspot_id ON accounts(hubspot_id);
      `);

      console.log('✅ PostgreSQL accounts table created successfully');
    } catch (error) {
      console.error('❌ Failed to setup accounts table:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  // Calculate actual BigQuery cost using real billing data
  calculateActualQueryCost(statistics) {
    // Use totalBytesBilled (actual billing amount) instead of totalBytesProcessed
    // BigQuery pricing: $6.25 per TB (updated 2024-06, was $5.00 previously)
    const costPerTB = 6.25;

    // Prefer totalBytesBilled for accurate billing, fallback to totalBytesProcessed
    const bytesBilled = parseInt(statistics.totalBytesBilled || statistics.totalBytesProcessed || 0);
    const tbBilled = bytesBilled / (1024 ** 4); // Convert bytes to TB

    return {
      actualCostUSD: (tbBilled * costPerTB).toFixed(6),
      bytesBilled: bytesBilled,
      bytesProcessed: parseInt(statistics.totalBytesProcessed || 0),
      pricingModel: statistics.totalBytesBilled ? 'actual-billed' : 'estimated-processed'
    };
  }

  // Get dry-run cost estimate before executing query
  async getDryRunCostEstimate(query) {
    if (!this.enableCostTracking) {
      return null;
    }

    try {
      const [job] = await this.bigquery.createQueryJob({
        query,
        location: 'US',
        dryRun: true // This returns cost estimate without executing
      });

      const metadata = job.metadata;
      const costData = this.calculateActualQueryCost(metadata.statistics || {});

      return {
        estimatedCostUSD: costData.actualCostUSD,
        estimatedBytesProcessed: costData.bytesProcessed,
        isDryRun: true
      };
    } catch (error) {
      console.warn(`⚠️  Dry-run cost estimation failed: ${error.message}`);
      return null;
    }
  }

  // Log cost metrics with enhanced real-time pricing data
  logCostMetrics(costInfo) {
    const timestamp = new Date().toISOString();

    if (this.costTrackingLevel === 'summary') {
      const pricingType = costInfo.pricingModel === 'actual-billed' ? 'ACTUAL' : 'EST';
      console.log(`💰 [COST-TRACKING] ${timestamp} | Job: ${costInfo.jobId} | Billed: ${costInfo.bytesBilled} bytes | ${pricingType} Cost: $${costInfo.actualCostUSD} | Duration: ${costInfo.duration}ms`);

      // Log dry-run estimate if available
      if (costInfo.dryRunEstimate) {
        console.log(`💰 [PRE-ESTIMATE] ${timestamp} | Estimated: $${costInfo.dryRunEstimate.estimatedCostUSD} (${costInfo.dryRunEstimate.estimatedBytesProcessed} bytes)`);
      }
    } else if (this.costTrackingLevel === 'detailed') {
      console.log(`💰 [COST-TRACKING-DETAILED] ${timestamp}`);
      console.log(`   📊 Job ID: ${costInfo.jobId}`);
      console.log(`   💰 Pricing Model: ${costInfo.pricingModel}`);
      console.log(`   📏 Bytes Billed: ${costInfo.bytesBilled} (${(costInfo.bytesBilled / (1024**3)).toFixed(2)} GB)`);
      console.log(`   📏 Bytes Processed: ${costInfo.bytesProcessed} (${(costInfo.bytesProcessed / (1024**3)).toFixed(2)} GB)`);
      console.log(`   ⚡ Slot Milliseconds: ${costInfo.totalSlotMs}`);
      console.log(`   ⏱️  Query Duration: ${costInfo.duration}ms`);
      console.log(`   💵 Actual Cost: $${costInfo.actualCostUSD}`);
      console.log(`   🔍 Query Preview: ${costInfo.queryPreview}`);
      console.log(`   ⏰ BigQuery Times: ${costInfo.startTime} → ${costInfo.endTime}`);

      if (costInfo.dryRunEstimate) {
        console.log(`   🧪 Pre-Run Estimate: $${costInfo.dryRunEstimate.estimatedCostUSD}`);
        const accuracy = ((parseFloat(costInfo.dryRunEstimate.estimatedCostUSD) / parseFloat(costInfo.actualCostUSD)) * 100).toFixed(1);
        console.log(`   🎯 Estimation Accuracy: ${accuracy}%`);
      }
    }
  }

  // Execute BigQuery with enhanced real-time cost tracking
  async executeQueryWithCostTracking(query, options = {}) {
    const startTime = Date.now();

    if (!this.enableCostTracking) {
      // Fallback to standard execution if cost tracking is disabled
      const [rows] = await this.bigquery.query({
        query,
        location: 'US',
        ...options
      });
      return rows;
    }

    // Step 1: Get dry-run cost estimate (pre-execution)
    const dryRunEstimate = await this.getDryRunCostEstimate(query);

    // Step 2: Execute actual query with job tracking
    const [job] = await this.bigquery.createQueryJob({
      query,
      location: 'US',
      dryRun: false,
      ...options
    });

    const [rows] = await job.getQueryResults();
    const metadata = job.metadata;

    // Step 3: Calculate actual cost using real BigQuery billing data
    const costData = this.calculateActualQueryCost(metadata.statistics || {});

    // Step 4: Compile comprehensive cost information
    const costInfo = {
      jobId: metadata.id,
      queryPreview: query.substring(0, 100).replace(/\s+/g, ' ') + '...',
      totalSlotMs: metadata.statistics?.totalSlotMs || '0',
      creationTime: metadata.statistics?.creationTime || null,
      startTime: metadata.statistics?.startTime || null,
      endTime: metadata.statistics?.endTime || null,
      duration: Date.now() - startTime,
      dryRunEstimate: dryRunEstimate,
      // Real cost data from BigQuery API
      ...costData
    };

    this.logCostMetrics(costInfo);
    return rows;
  }

  async getAccountsFromBigQuery() {
    console.log('🔍 Fetching accounts from BigQuery with dynamic rolling date range...');

    // Dynamic date range: 12 calendar months prior to current month + current month
    const now = new Date();
    const currentYear = now.getFullYear();
    const currentMonth = now.getMonth(); // 0-based (0 = January)

    // Calculate start date: first day of month 12 months ago
    const startDate = new Date(currentYear, currentMonth - 12, 1);
    const simulationStart = startDate.toISOString().split('T')[0];

    // Calculate end date: current date (updates daily)
    const simulationEnd = now.toISOString().split('T')[0];

    console.log(`📅 Dynamic date range: ${simulationStart} to ${simulationEnd} (rolling 12 months + current month)`)

    const query = `
      WITH account_unit_archive_dates AS (
        -- Get earliest unit archive dates for accounts (fallback archived_at logic from 2.1)
        SELECT
          u.account_id,
          MIN(u.archived_at) as earliest_unit_archived_at
        FROM units.units u
        WHERE u.status = 'ARCHIVED' AND u.archived_at IS NOT NULL
        GROUP BY u.account_id
      ),

      accounts_with_revenue AS (
        -- Find accounts with revenue during rolling 12-month window (including NULL launch dates)
        SELECT DISTINCT account_id
        FROM dbt_models.total_revenue_by_account_and_date
        WHERE date >= DATE('${simulationStart}')
          AND date <= DATE('${simulationEnd}')
          AND total > 0
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
      LEFT JOIN accounts_with_revenue awr ON a.id = awr.account_id
      WHERE
        (
          -- Account was launched before or during rolling window period
          (a.launched_at IS NOT NULL AND DATE(a.launched_at) <= DATE('${simulationEnd}'))
          OR
          -- OR account has revenue during rolling window period (catches NULL launch dates)
          awr.account_id IS NOT NULL
        )
        AND (
          -- Either account is still active
          a.status != 'ARCHIVED'
          OR (
            -- Or account was archived after rolling window start (include historical accounts)
            a.status = 'ARCHIVED'
            AND DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)) >= DATE('${simulationStart}')
          )
        )
        AND a.status IN ('ACTIVE', 'LAUNCHED', 'PAUSED', 'FROZEN', 'ARCHIVED')
      ORDER BY a.name
    `;

    const rows = await this.executeQueryWithCostTracking(query);
    console.log(`✅ Found ${rows.length} accounts (rolling 12-month window: ${simulationStart} to ${simulationEnd})`);
    return rows;
  }

  async populateAccounts() {
    await this.testConnection();
    await this.setupTables();

    const accounts = await this.getAccountsFromBigQuery();
    const client = await this.pool.connect();

    console.log('📝 Populating accounts table...');

    try {
      // Start transaction
      await client.query('BEGIN');

      // Use upsert (INSERT ... ON CONFLICT) instead of DELETE + INSERT
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

        // PostgreSQL upsert: insert or update if account_id already exists
        await client.query(`
          INSERT INTO accounts (
            account_id, account_name, status, launched_at,
            csm_owner, hubspot_id, archived_at, earliest_unit_archived_at, last_updated
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          ON CONFLICT (account_id) DO UPDATE SET
            account_name = CASE
              WHEN accounts.account_name_protected = TRUE THEN accounts.account_name
              ELSE EXCLUDED.account_name
            END,
            status = EXCLUDED.status,
            launched_at = EXCLUDED.launched_at,
            csm_owner = CASE
              WHEN accounts.csm_owner_protected = TRUE THEN accounts.csm_owner
              ELSE EXCLUDED.csm_owner
            END,
            hubspot_id = CASE
              WHEN accounts.hubspot_id_protected = TRUE THEN accounts.hubspot_id
              ELSE EXCLUDED.hubspot_id
            END,
            archived_at = EXCLUDED.archived_at,
            earliest_unit_archived_at = EXCLUDED.earliest_unit_archived_at,
            last_updated = EXCLUDED.last_updated
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

      // Commit transaction
      await client.query('COMMIT');
      console.log(`✅ Successfully populated ${accounts.length} accounts`);
      return accounts.length;

    } catch (error) {
      // Rollback on error
      await client.query('ROLLBACK');
      console.error('❌ Failed to populate accounts:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  async getAccountCount() {
    const client = await this.pool.connect();
    try {
      const result = await client.query('SELECT COUNT(*) as count FROM accounts');
      return parseInt(result.rows[0].count);
    } catch (error) {
      console.error('❌ Failed to get account count:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  async close() {
    await this.pool.end();
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const etl = new AccountsETLPostgresNative();
  etl.populateAccounts()
    .then(count => {
      console.log(`🎉 Accounts ETL completed! Populated ${count} accounts.`);
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Accounts ETL failed:', error);
      process.exit(1);
    })
    .finally(() => {
      etl.close();
    });
}

export { AccountsETLPostgresNative };