import { MonthlyBackfillOptimized } from './backfill-month-optimized.js';
import { MonthlyRollupETLPostgresNative } from './monthly-rollup-etl-postgres-native.js';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import path from 'path';

// Load environment variables from project root
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.join(__dirname, '../..');
dotenv.config({ path: path.join(projectRoot, '.env') });

// Set DATABASE_URL if not already set
if (!process.env.DATABASE_URL && process.env.EXTERNAL_DATABASE_URL) {
  process.env.DATABASE_URL = process.env.EXTERNAL_DATABASE_URL;
}

async function backfillAllHistorical() {
  console.log('🔄 Starting full historical backfill (Aug 2024 - Sep 2025)...\n');

  // Generate list of months to backfill
  const months = [
    '2024-08', '2024-09', '2024-10', '2024-11', '2024-12',
    '2025-01', '2025-02', '2025-03', '2025-04', '2025-05',
    '2025-06', '2025-07', '2025-08', '2025-09'
  ];

  const dailyBackfill = new MonthlyBackfillOptimized();
  const monthlyRollup = new MonthlyRollupETLPostgresNative();

  const results = [];
  let successCount = 0;
  let failCount = 0;

  for (const month of months) {
    try {
      console.log(`\n${'='.repeat(80)}`);
      console.log(`📅 Processing ${month}`);
      console.log('='.repeat(80));

      // Step 1: Backfill daily metrics for the entire month
      console.log(`\n🔹 Step 1/2: Daily metrics backfill for ${month}...`);
      const dailyResult = await dailyBackfill.backfillMonth(month);

      // Step 2: Run monthly rollup
      console.log(`\n🔹 Step 2/2: Monthly rollup for ${month}...`);

      // Current month (2025-09) uses regular monthly, others use historical
      const currentMonth = new Date().toISOString().slice(0, 7);
      let monthlyResult;

      if (month === currentMonth) {
        console.log(`   ℹ️  ${month} is current month - using current month rollup`);
        monthlyResult = await monthlyRollup.updateMonthlyMetrics(month);
      } else {
        console.log(`   ℹ️  ${month} is historical month - using historical rollup`);
        monthlyResult = await monthlyRollup.updateHistoricalMonthlyMetrics(month);
      }

      results.push({
        month,
        success: true,
        daily: dailyResult,
        monthly: monthlyResult
      });

      successCount++;

      console.log(`\n✅ ${month} completed successfully!`);
      console.log(`   Daily: ${dailyResult.totalProcessed} records (${dailyResult.updatedCount} updated, ${dailyResult.createdCount} created)`);
      console.log(`   Monthly: ${monthlyResult.accountsProcessed} accounts processed`);

    } catch (error) {
      console.error(`\n❌ ${month} failed:`, error.message);
      results.push({
        month,
        success: false,
        error: error.message
      });
      failCount++;

      // Continue with next month despite failure
      console.log(`\n⚠️  Continuing to next month despite failure...`);
    }
  }

  // Close connections
  await dailyBackfill.close();
  await monthlyRollup.close();

  // Summary
  console.log('\n' + '='.repeat(80));
  console.log('📊 FULL HISTORICAL BACKFILL SUMMARY');
  console.log('='.repeat(80));
  console.log(`✅ Successful: ${successCount}/${months.length} months`);
  console.log(`❌ Failed: ${failCount}/${months.length} months`);

  if (successCount > 0) {
    console.log(`\n✅ Successfully processed months:`);
    results.filter(r => r.success).forEach(r => {
      console.log(`   ${r.month}: ${r.daily.totalProcessed} daily records, ${r.monthly.accountsProcessed} accounts`);
    });
  }

  if (failCount > 0) {
    console.log(`\n❌ Failed months:`);
    results.filter(r => !r.success).forEach(r => {
      console.log(`   ${r.month}: ${r.error}`);
    });
  }

  // Calculate totals
  const totalDailyRecords = results.reduce((sum, r) => sum + (r.daily?.totalProcessed || 0), 0);
  const totalMonthlyAccounts = results.reduce((sum, r) => sum + (r.monthly?.accountsProcessed || 0), 0);

  console.log(`\n📈 Grand Totals:`);
  console.log(`   Daily records processed: ${totalDailyRecords.toLocaleString()}`);
  console.log(`   Monthly account-months: ${totalMonthlyAccounts}`);

  return { successCount, failCount, results };
}

// Run backfill
backfillAllHistorical()
  .then(({ successCount, failCount }) => {
    console.log('\n🎉 Full historical backfill completed!');
    process.exit(failCount > 0 ? 1 : 0);
  })
  .catch(error => {
    console.error('\n❌ Full historical backfill failed:', error);
    process.exit(1);
  });
