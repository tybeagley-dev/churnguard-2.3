import { AccountsETLSQLite } from './accounts-etl-sqlite.js';
import { DailySpendETLSQLite } from './daily-spend-etl-sqlite.js';
import { DailyTextsETLSQLite } from './daily-texts-etl-sqlite.js';
import { DailyCouponsETLSQLite } from './daily-coupons-etl-sqlite.js';
import { DailySubsETLSQLite } from './daily-subs-etl-sqlite.js';
import dotenv from 'dotenv';

dotenv.config();

class FullSimulationSQLite {
  constructor() {
    this.accountsETL = new AccountsETLSQLite();
    this.spendETL = new DailySpendETLSQLite();
    this.textsETL = new DailyTextsETLSQLite();
    this.couponsETL = new DailyCouponsETLSQLite();
    this.subsETL = new DailySubsETLSQLite();
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

  async runFullSimulation() {
    console.log('🌟 Starting ChurnGuard 2.2 PostgreSQL Simulation (SQLite)');
    console.log('=' .repeat(60));
    
    const startTime = Date.now();
    const startDate = process.env.SIMULATION_START_DATE || '2025-07-01';
    const endDate = process.env.SIMULATION_END_DATE || '2025-09-03';
    
    console.log(`📅 Simulating from ${startDate} to ${endDate}`);
    
    try {
      // Step 1: Set up accounts
      console.log('\n🏢 Step 1: Setting up accounts table...');
      const accountCount = await this.accountsETL.populateAccounts();
      console.log(`✅ ${accountCount} accounts loaded`);
      
      // Step 2: Generate date range
      const dates = this.generateDateRange(startDate, endDate);
      console.log(`\n📊 Step 2: Processing ${dates.length} days of metrics...`);
      
      let totalRecords = 0;
      let successfulDays = 0;
      let failedDays = [];
      
      // Step 3: Process each day
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
      const avgPerDay = (totalDuration / dates.length).toFixed(1);
      
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
      
      console.log(`\n🗄️  Database location: ${process.env.SQLITE_DB_PATH || './churnguard_simulation.db'}`);
      console.log('\n📋 View your data:');
      console.log('   sqlite3 churnguard_simulation.db');
      console.log('   .tables');
      console.log('   SELECT COUNT(*) FROM accounts;');
      console.log('   SELECT COUNT(*) FROM daily_metrics;');
      console.log('   SELECT * FROM daily_metrics WHERE date = "2025-07-01" LIMIT 5;');
      
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
    }
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const simulation = new FullSimulationSQLite();
  simulation.runFullSimulation()
    .then(result => {
      if (result.success) {
        console.log('\n🎯 Ready to build ChurnGuard 2.2 dashboard using SQLite!');
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

export { FullSimulationSQLite };