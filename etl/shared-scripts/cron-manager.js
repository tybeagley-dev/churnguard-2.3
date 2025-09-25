import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import path from 'path';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

class CronManager {
  constructor() {
    this.etlBasePath = path.join(__dirname, '..');
    this.logLevel = process.env.ETL_LOG_LEVEL || 'info';
    this.slackWebhook = process.env.SLACK_WEBHOOK_URL; // Optional: for notifications
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
        if (this.logLevel === 'debug') {
          process.stdout.write(output);
        }
      });

      child.stderr?.on('data', (data) => {
        const output = data.toString();
        stderr += output;
        if (this.logLevel === 'debug') {
          process.stderr.write(output);
        }
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

  async sendSlackNotification(message, isError = false) {
    if (!this.slackWebhook) return;

    const payload = {
      text: isError ? `🚨 ChurnGuard ETL Error: ${message}` : `✅ ChurnGuard ETL: ${message}`,
      username: 'ChurnGuard ETL Bot',
      channel: '#churnguard-alerts'
    };

    try {
      // Note: This would require a fetch implementation or http module
      this.log('debug', `Would send Slack notification: ${message}`);
    } catch (error) {
      this.log('warn', `Failed to send Slack notification: ${error.message}`);
    }
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
      // Step 1: Daily metrics ETL
      await this.runCommand('node', [
        path.join(this.etlBasePath, 'postgresql-native/daily-metrics-etl-postgres-native.js'),
        targetDate
      ]);

      this.log('info', `✅ Daily ETL completed successfully for ${targetDate}`);
      await this.sendSlackNotification(`Daily ETL completed for ${targetDate}`);

      return { success: true, date: targetDate };

    } catch (error) {
      this.log('error', `❌ Daily ETL failed for ${targetDate}: ${error.message}`);
      await this.sendSlackNotification(`Daily ETL failed for ${targetDate}: ${error.message}`, true);
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
      await this.sendSlackNotification(`Monthly rollup completed for ${targetMonth}`);

      return { success: true, month: targetMonth };

    } catch (error) {
      this.log('error', `❌ Monthly rollup failed for ${targetMonth}: ${error.message}`);
      await this.sendSlackNotification(`Monthly rollup failed for ${targetMonth}: ${error.message}`, true);
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

  // Full pipeline (daily + monthly if it's the 1st of month)
  async runFullPipeline(date = null) {
    const targetDate = date || this.getYesterdayDate();
    this.log('info', `🚀 Starting full ETL pipeline for ${targetDate}`);

    try {
      // Always run daily ETL
      await this.runDailyETL(targetDate);

      // Run monthly rollup if it's the 1st of the month
      const today = new Date();
      if (today.getDate() === 1) {
        this.log('info', `📅 First of month detected, running monthly rollup`);
        await this.runMonthlyRollup();
      }

      this.log('info', `🎉 Full ETL pipeline completed successfully`);
      return { success: true, date: targetDate };

    } catch (error) {
      this.log('error', `❌ Full ETL pipeline failed: ${error.message}`);
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

    default:
      console.error(`❌ Usage: node cron-manager.js <command> [date]

Commands:
  daily [YYYY-MM-DD]   Run daily ETL (defaults to yesterday)
  monthly [YYYY-MM]    Run monthly rollup (defaults to current month)
  test [YYYY-MM-DD]    Test ETL connections and dry run
  full [YYYY-MM-DD]    Run full pipeline (daily + monthly if 1st of month)

Examples:
  node cron-manager.js daily
  node cron-manager.js daily 2025-09-24
  node cron-manager.js monthly 2025-09
  node cron-manager.js test
  node cron-manager.js full

Environment Variables:
  ETL_LOG_LEVEL=debug|info|warn|error (default: info)
  SLACK_WEBHOOK_URL=<webhook> (optional: for notifications)
`);
      process.exit(1);
  }
}

export { CronManager };