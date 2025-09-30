import { DailyMetricsETLPostgresNative } from './daily-metrics-etl-postgres-native.js';
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

async function backfillSeptember() {
  console.log('🔄 Starting September 2025 backfill...\n');

  const etl = new DailyMetricsETLPostgresNative();
  const results = [];

  // Generate dates in September 2025 (Sept 1-29)
  const dates = [];
  for (let day = 1; day <= 29; day++) {
    const dateStr = `2025-09-${day.toString().padStart(2, '0')}`;
    dates.push(dateStr);
  }

  console.log(`📅 Processing ${dates.length} dates: ${dates[0]} to ${dates[dates.length - 1]}\n`);

  let successCount = 0;
  let failCount = 0;

  for (const date of dates) {
    try {
      console.log(`\n${'='.repeat(60)}`);
      console.log(`📊 Processing ${date}...`);
      console.log('='.repeat(60));

      const result = await etl.processDate(date);
      results.push({ date, success: true, ...result });
      successCount++;

      console.log(`✅ ${date} completed: ${result.totalProcessed} accounts, ${result.updatedCount} updated, ${result.createdCount} created`);

    } catch (error) {
      console.error(`❌ ${date} failed:`, error.message);
      results.push({ date, success: false, error: error.message });
      failCount++;
    }
  }

  // Summary
  console.log('\n' + '='.repeat(60));
  console.log('📊 BACKFILL SUMMARY');
  console.log('='.repeat(60));
  console.log(`✅ Successful: ${successCount}/${dates.length}`);
  console.log(`❌ Failed: ${failCount}/${dates.length}`);

  const totalAccounts = results.reduce((sum, r) => sum + (r.totalProcessed || 0), 0);
  const totalUpdated = results.reduce((sum, r) => sum + (r.updatedCount || 0), 0);
  const totalCreated = results.reduce((sum, r) => sum + (r.createdCount || 0), 0);

  console.log(`\n📈 Total Metrics:`);
  console.log(`   Accounts processed: ${totalAccounts}`);
  console.log(`   Records updated: ${totalUpdated}`);
  console.log(`   Records created: ${totalCreated}`);

  if (failCount > 0) {
    console.log(`\n⚠️  Failed dates:`);
    results.filter(r => !r.success).forEach(r => {
      console.log(`   ${r.date}: ${r.error}`);
    });
  }

  await etl.close();

  return { successCount, failCount, results };
}

// Run backfill
backfillSeptember()
  .then(({ successCount, failCount }) => {
    console.log('\n🎉 Backfill process completed!');
    process.exit(failCount > 0 ? 1 : 0);
  })
  .catch(error => {
    console.error('\n❌ Backfill process failed:', error);
    process.exit(1);
  });