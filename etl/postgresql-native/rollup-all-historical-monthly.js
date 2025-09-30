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

async function rollupAllHistoricalMonthly() {
  console.log('🔄 Starting monthly rollup for all historical months (Oct 2024 - Sep 2025)...\n');

  // Generate list of months to rollup (Oct 2024 - Sep 2025)
  const months = [
    '2024-10', '2024-11', '2024-12',
    '2025-01', '2025-02', '2025-03', '2025-04', '2025-05',
    '2025-06', '2025-07', '2025-08', '2025-09'
  ];

  const monthlyRollup = new MonthlyRollupETLPostgresNative();

  const results = [];
  let successCount = 0;
  let failCount = 0;

  for (const month of months) {
    try {
      console.log(`\n${'='.repeat(80)}`);
      console.log(`📅 Processing monthly rollup for ${month}`);
      console.log('='.repeat(80));

      const monthlyResult = await monthlyRollup.updateMonthlyMetrics(month);

      results.push({
        month,
        success: true,
        result: monthlyResult
      });

      successCount++;

      console.log(`\n✅ ${month} monthly rollup completed successfully!`);
      console.log(`   Accounts processed: ${monthlyResult.accountsProcessed}`);

    } catch (error) {
      console.error(`\n❌ ${month} monthly rollup failed:`, error.message);
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

  // Close connection
  await monthlyRollup.close();

  // Summary
  console.log('\n' + '='.repeat(80));
  console.log('📊 MONTHLY ROLLUP SUMMARY');
  console.log('='.repeat(80));
  console.log(`✅ Successful: ${successCount}/${months.length} months`);
  console.log(`❌ Failed: ${failCount}/${months.length} months`);

  if (successCount > 0) {
    console.log(`\n✅ Successfully processed months:`);
    results.filter(r => r.success).forEach(r => {
      console.log(`   ${r.month}: ${r.result.accountsProcessed} accounts`);
    });
  }

  if (failCount > 0) {
    console.log(`\n❌ Failed months:`);
    results.filter(r => !r.success).forEach(r => {
      console.log(`   ${r.month}: ${r.error}`);
    });
  }

  // Calculate totals
  const totalAccounts = results.reduce((sum, r) => sum + (r.result?.accountsProcessed || 0), 0);

  console.log(`\n📈 Grand Total:`);
  console.log(`   Total account-months processed: ${totalAccounts}`);

  return { successCount, failCount, results };
}

// Run rollup
rollupAllHistoricalMonthly()
  .then(({ successCount, failCount }) => {
    console.log('\n🎉 Monthly rollup for all historical months completed!');
    process.exit(failCount > 0 ? 1 : 0);
  })
  .catch(error => {
    console.error('\n❌ Monthly rollup failed:', error);
    process.exit(1);
  });
