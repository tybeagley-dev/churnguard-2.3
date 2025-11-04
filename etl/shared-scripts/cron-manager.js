import { spawn } from 'child_process';
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

class CronManager {
  constructor() {
    this.etlBasePath = path.join(__dirname, '..');
    this.logLevel = process.env.ETL_LOG_LEVEL || 'info';
  }

  log(level, message) {
    const timestamp = new Date().toISOString();
    const levels = { error: 0, warn: 1, info: 2, debug: 3 };
    const currentLevel = levels[this.logLevel] || 2;

    if (levels[level] <= currentLevel) {
      console.log(`[${timestamp}] [${level.toUpperCase()}] ${message}`);
    }
  }

  async runCommand(command, args, options = {}) {
    return new Promise((resolve, reject) => {
      this.log('info', `🚀 Executing: ${command} ${args.join(' ')}`);

      const child = spawn(command, args, {
        stdio: 'pipe',
        env: { ...process.env },
        ...options
      });

      let stdout = '';
      let stderr = '';

      child.stdout?.on('data', (data) => {
        const output = data.toString();
        stdout += output;
        // Always forward stdout to see cost tracking logs
        process.stdout.write(output);
      });

      child.stderr?.on('data', (data) => {
        const output = data.toString();
        stderr += output;
        // Always forward stderr for error visibility
        process.stderr.write(output);
      });

      child.on('close', (code) => {
        if (code === 0) {
          this.log('info', `✅ Command completed successfully`);
          resolve({ stdout, stderr, code });
        } else {
          this.log('error', `❌ Command failed with code ${code}`);
          this.log('error', `STDERR: ${stderr}`);
          reject(new Error(`Command failed with code ${code}: ${stderr}`));
        }
      });

      child.on('error', (error) => {
        this.log('error', `❌ Command error: ${error.message}`);
        reject(error);
      });
    });
  }


  // Get yesterday's date in YYYY-MM-DD format
  getYesterdayDate() {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    return yesterday.toISOString().split('T')[0];
  }

  // Get current month in YYYY-MM format
  getCurrentMonth() {
    return new Date().toISOString().slice(0, 7);
  }

  // Daily ETL Pipeline
  async runDailyETL(date = null) {
    const targetDate = date || this.getYesterdayDate();
    this.log('info', `🗓️  Starting daily ETL for ${targetDate}`);

    try {
      // Step 1: Accounts sync from BigQuery
      this.log('info', `🔄 Syncing accounts from BigQuery...`);
      await this.runCommand('node', [
        path.join(this.etlBasePath, 'postgresql-native/accounts-etl-postgres-native.js')
      ]);

      // Step 2: Daily metrics ETL
      this.log('info', `📊 Processing daily metrics for ${targetDate}...`);
      await this.runCommand('node', [
        path.join(this.etlBasePath, 'postgresql-native/daily-metrics-etl-postgres-native.js'),
        targetDate
      ]);

      this.log('info', `✅ Daily ETL completed successfully for ${targetDate}`);

      return { success: true, date: targetDate };

    } catch (error) {
      this.log('error', `❌ Daily ETL failed for ${targetDate}: ${error.message}`);
      throw error;
    }
  }

  // Monthly rollup pipeline
  async runMonthlyRollup(month = null) {
    const targetMonth = month || this.getCurrentMonth();
    this.log('info', `📅 Starting monthly rollup for ${targetMonth}`);

    try {
      // Monthly aggregation
      await this.runCommand('node', [
        path.join(this.etlBasePath, 'postgresql-native/monthly-rollup-etl-postgres-native.js'),
        targetMonth
      ]);

      this.log('info', `✅ Monthly rollup completed successfully for ${targetMonth}`);

      return { success: true, month: targetMonth };

    } catch (error) {
      this.log('error', `❌ Monthly rollup failed for ${targetMonth}: ${error.message}`);
      throw error;
    }
  }

  // Get previous month in YYYY-MM format
  getPreviousMonth() {
    const lastMonth = new Date();
    lastMonth.setMonth(lastMonth.getMonth() - 1);
    return lastMonth.toISOString().slice(0, 7);
  }

  // Historical monthly rollup for previous month (runs on 1st of month)
  async runHistoricalMonthlyRollup(month = null) {
    const targetMonth = month || this.getPreviousMonth();
    this.log('info', `📜 Starting historical monthly rollup for ${targetMonth}`);

    try {
      // Historical monthly aggregation for previous month
      await this.runCommand('node', [
        path.join(this.etlBasePath, 'postgresql-native/monthly-rollup-etl-postgres-native.js'),
        targetMonth,
        '--historical'
      ]);

      this.log('info', `✅ Historical monthly rollup completed successfully for ${targetMonth}`);

      return { success: true, month: targetMonth };

    } catch (error) {
      this.log('error', `❌ Historical monthly rollup failed for ${targetMonth}: ${error.message}`);
      throw error;
    }
  }

  // Test connection and dry run
  async testETLs(date = null) {
    const targetDate = date || this.getYesterdayDate();
    this.log('info', `🧪 Testing ETL connections and dry run for ${targetDate}`);

    try {
      // Test daily metrics ETL with --dry-run flag
      await this.runCommand('node', [
        path.join(this.etlBasePath, 'postgresql-native/daily-metrics-etl-postgres-native.js'),
        targetDate,
        '--dry-run'
      ]);

      this.log('info', `✅ ETL test completed successfully`);
      return { success: true, date: targetDate };

    } catch (error) {
      this.log('error', `❌ ETL test failed: ${error.message}`);
      throw error;
    }
  }

  // Full pipeline (daily + smart month rollup detection)
  async runFullPipeline(date = null) {
    const targetDate = date || this.getYesterdayDate();
    this.log('info', `🚀 Starting full ETL pipeline for ${targetDate}`);

    try {
      // Always run daily ETL
      await this.runDailyETL(targetDate);

      // Smart month rollup: handle cross-month scenarios
      const targetMonth = targetDate.slice(0, 7); // YYYY-MM from target date
      const currentMonth = this.getCurrentMonth();

      if (targetMonth !== currentMonth) {
        // Processing previous month data - rollup both months
        this.log('info', `📅 Cross-month detected: processing ${targetMonth} and ${currentMonth}`);
        this.log('info', `🔄 Updating previous month ${targetMonth} due to new data`);
        await this.runMonthlyRollup(targetMonth); // Previous month
        this.log('info', `📅 Running current month rollup for trending updates`);
        await this.runMonthlyRollup(currentMonth); // Current month
      } else {
        // Same month - standard rollup
        this.log('info', `📅 Running current month rollup for trending updates`);
        await this.runMonthlyRollup();
      }

      this.log('info', `🎉 Full ETL pipeline completed successfully`);
      return { success: true, date: targetDate };

    } catch (error) {
      this.log('error', `❌ Full ETL pipeline failed: ${error.message}`);
      throw error;
    }
  }

  // HubSpot sync pipeline
  async runHubSpotSync() {
    this.log('info', `🔄 Starting HubSpot sync`);

    try {
      // Run HubSpot sync script
      await this.runCommand('node', [
        path.join(this.etlBasePath, 'postgresql-native/hubspot-sync.js')
      ]);

      this.log('info', `✅ HubSpot sync completed successfully`);
      return { success: true };

    } catch (error) {
      this.log('error', `❌ HubSpot sync failed: ${error.message}`);
      throw error;
    }
  }

  // Month-end backfill for archived accounts
  async runMonthEndBackfill() {
    this.log('info', `🔄 Starting month-end MSA backfill for archived accounts`);

    try {
      // Run month-end backfill script
      await this.runCommand('node', [
        path.join(this.etlBasePath, 'postgresql-native/backfill-archived-month-end.js')
      ]);

      this.log('info', `✅ Month-end backfill completed successfully`);
      return { success: true };

    } catch (error) {
      this.log('error', `❌ Month-end backfill failed: ${error.message}`);
      throw error;
    }
  }
}

// CLI Interface
if (import.meta.url === `file://${process.argv[1]}`) {
  const command = process.argv[2];
  const dateArg = process.argv[3];

  const cronManager = new CronManager();

  switch (command) {
    case 'daily':
      cronManager.runDailyETL(dateArg)
        .then(() => process.exit(0))
        .catch(() => process.exit(1));
      break;

    case 'monthly':
      cronManager.runMonthlyRollup(dateArg)
        .then(() => process.exit(0))
        .catch(() => process.exit(1));
      break;

    case 'historical':
      cronManager.runHistoricalMonthlyRollup(dateArg)
        .then(() => process.exit(0))
        .catch(() => process.exit(1));
      break;

    case 'test':
      cronManager.testETLs(dateArg)
        .then(() => process.exit(0))
        .catch(() => process.exit(1));
      break;

    case 'full':
      cronManager.runFullPipeline(dateArg)
        .then(() => process.exit(0))
        .catch(() => process.exit(1));
      break;

    case 'hubspot':
      cronManager.runHubSpotSync()
        .then(() => process.exit(0))
        .catch(() => process.exit(1));
      break;

    case 'month-end-backfill':
      cronManager.runMonthEndBackfill()
        .then(() => process.exit(0))
        .catch(() => process.exit(1));
      break;

    default:
      console.error(`❌ Usage: node cron-manager.js <command> [date]

Commands:
  daily [YYYY-MM-DD]      Run daily ETL (defaults to yesterday)
  monthly [YYYY-MM]       Run monthly rollup for current month trending (defaults to current month)
  historical [YYYY-MM]    Run historical monthly rollup for previous month (defaults to previous month)
  test [YYYY-MM-DD]       Test ETL connections and dry run
  full [YYYY-MM-DD]       Run full pipeline (daily + current month rollup)
  hubspot                 Sync account risk data to HubSpot
  month-end-backfill      Backfill missing MSA data for archived accounts

Examples:
  node cron-manager.js daily
  node cron-manager.js daily 2025-09-24
  node cron-manager.js monthly 2025-09
  node cron-manager.js historical 2025-08
  node cron-manager.js test
  node cron-manager.js full
  node cron-manager.js hubspot

Environment Variables:
  ETL_LOG_LEVEL=debug|info|warn|error (default: info)
`);
      process.exit(1);
  }
}

export { CronManager };