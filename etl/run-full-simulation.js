import { AccountsETL } from './accounts-etl.js';
import { DailySpendETL } from './daily-spend-etl.js';
import { DailyTextsETL } from './daily-texts-etl.js';
import { DailyCouponsETL } from './daily-coupons-etl.js';
import { DailySubsETL } from './daily-subs-etl.js';
import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

class FullSimulation {
  constructor() {
    this.accountsETL = new AccountsETL();
    this.spendETL = new DailySpendETL();
    this.textsETL = new DailyTextsETL();
    this.couponsETL = new DailyCouponsETL();
    this.subsETL = new DailySubsETL();
    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
    });
  }

  async getDatabase() {
    return this.pool;
  }

  async getMostRecentDate() {
    const client = await this.pool.connect();

    try {
      const result = await client.query(`
        SELECT MAX(date) as most_recent_date
        FROM daily_metrics
      `);

      if (result.rows[0] && result.rows[0].most_recent_date) {
        console.log(`📅 Most recent data in database: ${result.rows[0].most_recent_date}`);
        return result.rows[0].most_recent_date;
      }

      console.log(`📅 No existing data found, starting from environment default`);
      return null;

    } catch (error) {
      console.log(`📅 No daily_metrics table found, starting fresh`);
      return null;
    } finally {
      client.release();
    }
  }

  addDays(dateStr, days) {
    const date = new Date(dateStr);
    date.setDate(date.getDate() + days);
    return date.toISOString().split('T')[0];
  }

  generateDateRange(startDate, endDate) {
    const dates = [];
    const start = new Date(startDate);
    const end = new Date(endDate);

    for (let date = new Date(start); date <= end; date.setDate(date.getDate() + 1)) {
      dates.push(date.toISOString().split('T')[0]);
    }

    return dates;
  }

  async simulateDay(date) {
    console.log(`🚀 Processing ${date}...`);
    const startTime = Date.now();

    try {
      // Run all ETL scripts in parallel for maximum speed
      const [spendResult, textsResult, couponsResult, subsResult] = await Promise.all([
        this.spendETL.processDate(date).catch(err => {
          console.warn(`⚠️  Spend ETL warning for ${date}:`, err.message);
          return { updatedCount: 0, createdCount: 0, error: err.message };
        }),
        this.textsETL.processDate(date).catch(err => {
          console.warn(`⚠️  Texts ETL warning for ${date}:`, err.message);
          return { updatedCount: 0, createdCount: 0, error: err.message };
        }),
        this.couponsETL.processDate(date).catch(err => {
          console.warn(`⚠️  Coupons ETL warning for ${date}:`, err.message);
          return { updatedCount: 0, createdCount: 0, error: err.message };
        }),
        this.subsETL.processDate(date).catch(err => {
          console.warn(`⚠️  Subs ETL warning for ${date}:`, err.message);
          return { updatedCount: 0, createdCount: 0, error: err.message };
        })
      ]);

      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      const totalRecords = (
        spendResult.updatedCount + spendResult.createdCount +
        textsResult.updatedCount + textsResult.createdCount +
        couponsResult.updatedCount + couponsResult.createdCount +
        subsResult.updatedCount + subsResult.createdCount
      );

      console.log(`   ✅ ${date}: ${totalRecords} records in ${duration}s`);

      return {
        date,
        duration,
        totalRecords,
        spend: spendResult,
        texts: textsResult,
        coupons: couponsResult,
        subs: subsResult
      };

    } catch (error) {
      console.error(`❌ Daily simulation failed for ${date}:`, error);
      throw error;
    }
  }

  getYesterday() {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    return yesterday.toISOString().split('T')[0];
  }

  async runFullSimulation() {
    console.log('🌟 Starting ChurnGuard 2.3 ETL with Gap Detection (PostgreSQL)');
    console.log('=' .repeat(60));

    const startTime = Date.now();
    const envStartDate = process.env.SIMULATION_START_DATE || '2025-07-01';
    const endDate = process.env.SIMULATION_END_DATE || this.getYesterday();

    try {
      // Step 1: Set up accounts
      console.log('\n🏢 Step 1: Setting up accounts table...');
      const accountCount = await this.accountsETL.populateAccounts();
      console.log(`✅ ${accountCount} accounts loaded`);

      // Step 2: Check for existing data and determine start date
      console.log('\n🔍 Step 2: Checking for existing data...');
      const mostRecentDate = await this.getMostRecentDate();

      let actualStartDate;
      if (mostRecentDate) {
        // Start from the day after the most recent data
        actualStartDate = this.addDays(mostRecentDate, 1);
        console.log(`🎯 Gap detected: Resuming from ${actualStartDate}`);
        console.log(`   (Previous data ends at ${mostRecentDate})`);
      } else {
        // No existing data, use environment default
        actualStartDate = envStartDate;
        console.log(`🚀 Fresh start: Beginning from ${actualStartDate}`);
      }

      // Check if we're already up to date
      if (actualStartDate > endDate) {
        console.log(`✅ Database is already up to date! (Latest: ${mostRecentDate}, Target: ${endDate})`);
        return {
          success: true,
          message: 'Already up to date',
          totalRecords: 0,
          successfulDays: 0,
          failedDays: [],
          duration: 0
        };
      }

      // Step 3: Generate date range from actual start
      const dates = this.generateDateRange(actualStartDate, endDate);
      console.log(`\n📊 Step 3: Processing ${dates.length} days of metrics (${actualStartDate} to ${endDate})...`);

      let totalRecords = 0;
      let successfulDays = 0;
      let failedDays = [];

      // Step 4: Process each day
      for (let i = 0; i < dates.length; i++) {
        const date = dates[i];
        const progress = `(${i + 1}/${dates.length})`;

        try {
          console.log(`📅 ${progress} ${date}:`);
          const result = await this.simulateDay(date);

          totalRecords += result.totalRecords;
          successfulDays++;

        } catch (error) {
          console.error(`❌ Failed to process ${date}:`, error.message);
          failedDays.push(date);
        }
      }

      // Final summary
      const totalDuration = ((Date.now() - startTime) / 1000).toFixed(1);
      const avgPerDay = dates.length > 0 ? (totalDuration / dates.length).toFixed(1) : '0';

      console.log('\n' + '=' .repeat(60));
      console.log('🎉 SIMULATION COMPLETED!');
      console.log('=' .repeat(60));
      console.log(`⏱️  Total time: ${totalDuration}s (avg ${avgPerDay}s/day)`);
      console.log(`📊 Total records processed: ${totalRecords.toLocaleString()}`);
      console.log(`✅ Successful days: ${successfulDays}/${dates.length}`);
      console.log(`❌ Failed days: ${failedDays.length}`);

      if (failedDays.length > 0) {
        console.log(`Failed dates: ${failedDays.join(', ')}`);
      }

      console.log(`\n🗄️  Database: ${process.env.DATABASE_URL ? 'PostgreSQL (Production)' : 'Not configured'}`);
      console.log('\n📋 Ready for production deployment!');

      return {
        success: true,
        totalRecords,
        successfulDays,
        failedDays,
        duration: totalDuration
      };

    } catch (error) {
      console.error('❌ Full simulation failed:', error);
      return {
        success: false,
        error: error.message
      };
    } finally {
      // Close the pool when done
      await this.pool.end();
    }
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const simulation = new FullSimulation();
  simulation.runFullSimulation()
    .then(result => {
      if (result.success) {
        console.log('\n🎯 ChurnGuard 2.3 Production ETL completed successfully!');
        process.exit(0);
      } else {
        console.error('❌ Simulation failed');
        process.exit(1);
      }
    })
    .catch(error => {
      console.error('❌ Unexpected error:', error);
      process.exit(1);
    });
}

export { FullSimulation };