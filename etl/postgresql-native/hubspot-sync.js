import { fileURLToPath } from 'url';
import path from 'path';
import dotenv from 'dotenv';
import fs from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load environment from project root .env file
const projectRoot = path.join(__dirname, '../..');
const envPath = path.join(projectRoot, '.env');

if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath });
} else {
  dotenv.config();
}

// Set DATABASE_URL if not already set (for PostgreSQL connection)
if (!process.env.DATABASE_URL && process.env.EXTERNAL_DATABASE_URL) {
  process.env.DATABASE_URL = process.env.EXTERNAL_DATABASE_URL;
}

// Import HubSpot sync service from src directory
import { HubSpotSyncService } from '../../src/services/hubspot-sync.js';

async function runHubSpotSync() {
  const targetDate = process.argv[2]; // Optional date parameter
  const isDryRun = process.argv.includes('--dry-run');

  console.log(`🔄 Starting HubSpot sync${isDryRun ? ' (DRY RUN)' : ''}`);

  if (isDryRun) {
    console.log(`🧪 DRY RUN MODE: No actual updates will be sent to HubSpot`);
  }

  try {
    const syncService = new HubSpotSyncService();

    if (isDryRun) {
      // Test connection only for dry run
      const connectionTest = await syncService.testHubSpotConnection();
      if (connectionTest.success) {
        console.log(`✅ HubSpot connection test successful`);
        console.log(`📊 Ready to sync accounts to HubSpot`);
      } else {
        console.log(`❌ HubSpot connection test failed: ${connectionTest.message}`);
        process.exit(1);
      }
    } else {
      // Run actual sync
      const result = await syncService.syncAccountsToHubSpot(targetDate, 'cron');

      if (result.success) {
        console.log(`🎉 HubSpot sync completed successfully`);
        console.log(`📊 Sync Summary:`);
        console.log(`   - Process Date: ${result.processDate}`);
        console.log(`   - Sync Mode: ${result.syncMode}`);
        console.log(`   - Total Accounts: ${result.totalAccounts}`);
        console.log(`   - Successful Syncs: ${result.successfulSyncs}`);
        console.log(`   - Failed Syncs: ${result.failedSyncs}`);
        console.log(`   - ID Translations: ${result.translatedCount}`);
        console.log(`   - Risk Breakdown: High: ${result.summary.highRiskSynced}, Medium: ${result.summary.mediumRiskSynced}, Low: ${result.summary.lowRiskSynced}`);
      } else {
        console.error(`❌ HubSpot sync failed`);
        process.exit(1);
      }
    }

  } catch (error) {
    console.error(`❌ HubSpot sync error:`, error.message);
    if (process.env.ETL_LOG_LEVEL === 'debug') {
      console.error(error.stack);
    }
    process.exit(1);
  }
}

// CLI Interface
if (import.meta.url === `file://${process.argv[1]}`) {
  runHubSpotSync()
    .then(() => process.exit(0))
    .catch(() => process.exit(1));
}

export { runHubSpotSync };