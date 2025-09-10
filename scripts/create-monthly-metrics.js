import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

class MonthlyMetricsBuilder {
  constructor() {
    this.dbPath = process.env.SQLITE_DB_PATH || './data/churnguard_simulation.db';
  }

  async getDatabase() {
    const db = await open({
      filename: this.dbPath,
      driver: sqlite3.Database
    });
    return db;
  }

  async createMonthlyMetricsTable() {
    console.log('📊 Creating monthly_metrics table...');
    const db = await this.getDatabase();
    
    // Create monthly_metrics table
    await db.exec(`
      CREATE TABLE IF NOT EXISTS monthly_metrics (
        account_id TEXT,
        month TEXT,  -- YYYY-MM format
        month_label TEXT,  -- e.g., "August 2025"
        total_spend REAL DEFAULT 0,
        total_texts_delivered INTEGER DEFAULT 0,
        total_coupons_redeemed INTEGER DEFAULT 0,
        avg_active_subs_cnt INTEGER DEFAULT 0,  -- Average subscribers for the month
        days_with_activity INTEGER DEFAULT 0,  -- How many days in the month had activity
        last_updated TEXT,
        PRIMARY KEY (account_id, month),
        FOREIGN KEY (account_id) REFERENCES accounts (account_id)
      );
    `);

    // Create indexes for performance
    await db.exec(`
      CREATE INDEX IF NOT EXISTS idx_monthly_metrics_month ON monthly_metrics(month);
      CREATE INDEX IF NOT EXISTS idx_monthly_metrics_account ON monthly_metrics(account_id);
    `);

    await db.close();
    console.log('✅ Monthly metrics table created successfully');
  }

  async populateMonthlyMetrics() {
    console.log('📊 Populating monthly_metrics from daily_metrics...');
    const db = await this.getDatabase();

    // Clear existing data
    await db.exec('DELETE FROM monthly_metrics');

    // Aggregate daily data into monthly metrics
    const insertQuery = `
      INSERT INTO monthly_metrics (
        account_id, 
        month, 
        month_label,
        total_spend, 
        total_texts_delivered, 
        total_coupons_redeemed, 
        avg_active_subs_cnt,
        days_with_activity,
        last_updated
      )
      SELECT 
        account_id,
        substr(date, 1, 7) as month,  -- Extract YYYY-MM
        -- Create month label (e.g., "August 2025")
        CASE substr(date, 6, 2)
          WHEN '01' THEN 'January'
          WHEN '02' THEN 'February'
          WHEN '03' THEN 'March'
          WHEN '04' THEN 'April'
          WHEN '05' THEN 'May'
          WHEN '06' THEN 'June'
          WHEN '07' THEN 'July'
          WHEN '08' THEN 'August'
          WHEN '09' THEN 'September'
          WHEN '10' THEN 'October'
          WHEN '11' THEN 'November'
          WHEN '12' THEN 'December'
        END || ' ' || substr(date, 1, 4) as month_label,
        
        -- Aggregate the metrics
        ROUND(SUM(total_spend), 2) as total_spend,
        SUM(total_texts_delivered) as total_texts_delivered,
        SUM(coupons_redeemed) as total_coupons_redeemed,
        ROUND(AVG(active_subs_cnt), 0) as avg_active_subs_cnt,
        COUNT(*) as days_with_activity,
        datetime('now') as last_updated
        
      FROM daily_metrics
      GROUP BY account_id, substr(date, 1, 7)
      ORDER BY account_id, month
    `;

    await db.exec(insertQuery);

    // Get counts for validation
    const totalRecords = await db.get('SELECT COUNT(*) as count FROM monthly_metrics');
    const monthCount = await db.get('SELECT COUNT(DISTINCT month) as count FROM monthly_metrics');
    const accountCount = await db.get('SELECT COUNT(DISTINCT account_id) as count FROM monthly_metrics');

    await db.close();

    console.log(`✅ Monthly metrics populated successfully:`);
    console.log(`   📊 Total records: ${totalRecords.count.toLocaleString()}`);
    console.log(`   📅 Months covered: ${monthCount.count}`);
    console.log(`   🏢 Accounts: ${accountCount.count}`);
  }

  async validateMonthlyMetrics() {
    console.log('🔍 Validating monthly_metrics data...');
    const db = await this.getDatabase();

    // Sample validation queries
    const sampleData = await db.all(`
      SELECT 
        month,
        month_label,
        COUNT(*) as accounts,
        ROUND(SUM(total_spend), 2) as total_spend,
        SUM(total_texts_delivered) as total_texts,
        SUM(total_coupons_redeemed) as total_coupons
      FROM monthly_metrics 
      GROUP BY month, month_label 
      ORDER BY month DESC
      LIMIT 5
    `);

    console.log('\n📊 Monthly Metrics Summary (Latest 5 months):');
    sampleData.forEach(row => {
      console.log(`${row.month_label}: ${row.accounts} accounts, $${row.total_spend.toLocaleString()}, ${row.total_texts.toLocaleString()} texts, ${row.total_coupons.toLocaleString()} coupons`);
    });

    // Validate against daily data for one month
    const comparison = await db.get(`
      SELECT 
        -- Monthly metrics
        (SELECT SUM(total_spend) FROM monthly_metrics WHERE month = '2025-07') as monthly_spend,
        (SELECT SUM(total_texts_delivered) FROM monthly_metrics WHERE month = '2025-07') as monthly_texts,
        -- Daily metrics
        (SELECT SUM(total_spend) FROM daily_metrics WHERE substr(date, 1, 7) = '2025-07') as daily_spend,
        (SELECT SUM(total_texts_delivered) FROM daily_metrics WHERE substr(date, 1, 7) = '2025-07') as daily_texts
    `);

    console.log('\n🔍 Data Integrity Check (July 2025):');
    console.log(`   Monthly table spend: $${comparison.monthly_spend?.toLocaleString() || 0}`);
    console.log(`   Daily table spend: $${comparison.daily_spend?.toLocaleString() || 0}`);
    console.log(`   Monthly table texts: ${comparison.monthly_texts?.toLocaleString() || 0}`);
    console.log(`   Daily table texts: ${comparison.daily_texts?.toLocaleString() || 0}`);
    
    const spendMatch = Math.abs((comparison.monthly_spend || 0) - (comparison.daily_spend || 0)) < 0.01;
    const textsMatch = (comparison.monthly_texts || 0) === (comparison.daily_texts || 0);
    
    console.log(`   Spend match: ${spendMatch ? '✅' : '❌'}`);
    console.log(`   Texts match: ${textsMatch ? '✅' : '❌'}`);

    await db.close();
  }

  async run() {
    try {
      await this.createMonthlyMetricsTable();
      await this.populateMonthlyMetrics();
      await this.validateMonthlyMetrics();
      
      console.log('\n🎉 Monthly metrics table ready!');
      console.log('   This table aggregates daily metrics by account and month for faster queries.');
      console.log('   Use this for monthly views and historical reporting.');
      
    } catch (error) {
      console.error('❌ Error creating monthly metrics:', error);
      throw error;
    }
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const builder = new MonthlyMetricsBuilder();
  builder.run()
    .then(() => {
      console.log('\n✅ Monthly metrics creation completed!');
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Monthly metrics creation failed:', error);
      process.exit(1);
    });
}

export { MonthlyMetricsBuilder };