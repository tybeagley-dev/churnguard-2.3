import { DailySpendETLSQLite } from './daily-spend-etl-sqlite.js';
import { DailyTextsETLSQLite } from './daily-texts-etl-sqlite.js';
import { DailyCouponsETLSQLite } from './daily-coupons-etl-sqlite.js';
import { DailySubsETLSQLite } from './daily-subs-etl-sqlite.js';
import dotenv from 'dotenv';

dotenv.config();

class DailyBigQuerySync {
  constructor() {
    this.spendETL = new DailySpendETLSQLite();
    this.textsETL = new DailyTextsETLSQLite();
    this.couponsETL = new DailyCouponsETLSQLite();
    this.subsETL = new DailySubsETLSQLite();
  }

  async syncDay(date) {
    console.log(`🚀 Starting daily BigQuery sync for ${date}...`);
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
      
      console.log(`\n✅ Daily BigQuery sync completed for ${date} in ${duration}s`);
      console.log(`📊 Results:`);
      console.log(`   💰 Spend: ${spendResult.updatedCount + spendResult.createdCount} records`);
      console.log(`   📱 Texts: ${textsResult.updatedCount + textsResult.createdCount} records`);
      console.log(`   🎫 Coupons: ${couponsResult.updatedCount + couponsResult.createdCount} records`);
      console.log(`   👥 Subscribers: ${subsResult.updatedCount + subsResult.createdCount} records`);

      return {
        date,
        duration,
        spend: spendResult,
        texts: textsResult,
        coupons: couponsResult,
        subs: subsResult
      };
      
    } catch (error) {
      console.error(`❌ Daily BigQuery sync failed for ${date}:`, error);
      throw error;
    }
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const date = process.argv[2];
  if (!date) {
    console.error('❌ Usage: node run-daily-simulation.js YYYY-MM-DD');
    console.error('Example: node run-daily-simulation.js 2025-07-01');
    process.exit(1);
  }

  const sync = new DailyBigQuerySync();
  sync.syncDay(date)
    .then(result => {
      console.log(`🎉 BigQuery sync completed successfully!`);
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Simulation failed:', error);
      process.exit(1);
    });
}

export { DailySimulation };