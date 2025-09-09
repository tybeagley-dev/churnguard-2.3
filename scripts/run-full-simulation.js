import { AccountsETL } from './accounts-etl.js';
import { DailySimulation } from './run-daily-simulation.js';
import dotenv from 'dotenv';

dotenv.config();

class FullSimulation {
  constructor() {
    this.accountsETL = new AccountsETL();
    this.dailySimulation = new DailySimulation();
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

  async runFullSimulation() {
    console.log('🌟 Starting ChurnGuard 2.2 PostgreSQL Simulation');
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
          console.log(`\n📅 ${progress} Processing ${date}...`);
          const result = await this.dailySimulation.simulateDay(date);
          
          const dayRecords = (
            result.spend.updatedCount + result.spend.createdCount +
            result.texts.updatedCount + result.texts.createdCount +
            result.coupons.updatedCount + result.coupons.createdCount +
            result.subs.updatedCount + result.subs.createdCount
          );
          
          totalRecords += dayRecords;
          successfulDays++;
          
        } catch (error) {
          console.error(`❌ Failed to process ${date}:`, error.message);
          failedDays.push(date);
        }
      }
      
      // Final summary
      const totalDuration = ((Date.now() - startTime) / 1000).toFixed(1);
      console.log('\n' + '=' .repeat(60));
      console.log('🎉 SIMULATION COMPLETED!');
      console.log('=' .repeat(60));
      console.log(`⏱️  Total time: ${totalDuration}s`);
      console.log(`📊 Total records processed: ${totalRecords.toLocaleString()}`);
      console.log(`✅ Successful days: ${successfulDays}/${dates.length}`);
      console.log(`❌ Failed days: ${failedDays.length}`);
      
      if (failedDays.length > 0) {
        console.log(`Failed dates: ${failedDays.join(', ')}`);
      }
      
      console.log('\n🔗 Your Google Sheet is ready:');
      console.log(`https://docs.google.com/spreadsheets/d/${process.env.GOOGLE_SHEETS_ID}/edit`);
      
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
  const simulation = new FullSimulation();
  simulation.runFullSimulation()
    .then(result => {
      if (result.success) {
        console.log('🎯 Ready to build ChurnGuard 2.2 dashboard!');
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