import { DailySpendETLSQLite } from './daily-spend-etl-sqlite.js';
import { DailyTextsETLSQLite } from './daily-texts-etl-sqlite.js';
import { DailyCouponsETLSQLite } from './daily-coupons-etl-sqlite.js';
import { DailySubsETLSQLite } from './daily-subs-etl-sqlite.js';
import { AccountsETLSQLite } from './accounts-etl-sqlite.js';
import { HistoricalRiskPopulator } from '../populate-historical-risk-levels.js';
import { BigQuery } from '@google-cloud/bigquery';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

export class DailyProductionETL {
  constructor() {
    this.dbPath = process.env.SQLITE_DB_PATH || './data/churnguard_simulation.db';
    this.spendETL = new DailySpendETLSQLite();
    this.textsETL = new DailyTextsETLSQLite();
    this.couponsETL = new DailyCouponsETLSQLite();
    this.subsETL = new DailySubsETLSQLite();
    this.accountsETL = new AccountsETLSQLite();
    this.bigquery = new BigQuery({
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
    });
    
    // Complete 8-flag risk calculation thresholds
    this.MONTHLY_REDEMPTIONS_THRESHOLD = 10;
    this.LOW_ENGAGEMENT_COMBO_SUBS_THRESHOLD = 300;
    this.LOW_ENGAGEMENT_COMBO_REDEMPTIONS_THRESHOLD = 35;
    this.LOW_ACTIVITY_SUBS_THRESHOLD = 300;
    this.SPEND_DROP_THRESHOLD = 0.40; // 40%
    this.REDEMPTIONS_DROP_THRESHOLD = 0.50; // 50%
  }

  async getDatabase() {
    const db = await open({
      filename: this.dbPath,
      driver: sqlite3.Database
    });
    return db;
  }

  getYesterday() {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    return yesterday.toISOString().split('T')[0];
  }

  getCurrentMonth() {
    const today = new Date();
    return today.toISOString().substring(0, 7); // YYYY-MM
  }

  // Dynamic month label generation - Phase 1 of migration plan
  formatMonthLabel(monthString) {
    const [year, month] = monthString.split('-');
    const monthNames = ['January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December'];
    const monthIndex = parseInt(month) - 1;
    return `${monthNames[monthIndex]} ${year}`;
  }

  async runDailyPipeline(targetDate = null) {
    const processDate = targetDate || this.getYesterday();
    const startTime = Date.now();
    
    console.log(`🌅 ${new Date().toISOString()} - Starting Production Daily ETL Pipeline`);
    console.log(`📅 Processing date: ${processDate}`);
    
    try {
      // Step 0: Check for month-end and complete previous month's historical risk levels
      const monthEndResults = await this.checkAndCompleteMonthEnd(processDate);
      
      // Step 1: Update accounts table from BigQuery
      console.log('\n👥 Step 1: Update accounts table from BigQuery');
      const accountsResults = await this.updateAccountsTable();
      
      // Step 2: Extract from BigQuery and Load to daily_metrics
      console.log('\n📊 Step 2: BigQuery Extract & Load to daily_metrics');
      const extractResults = await this.extractAndLoadDailyMetrics(processDate);
      
      // Step 3: Aggregate to monthly_metrics (full MTD recalculation)
      console.log('\n📈 Step 3: Aggregate to monthly_metrics');
      const monthlyResults = await this.aggregateToMonthlyMetrics(processDate);
      
      // Step 4: Update trending risk levels (proportional analysis)
      console.log('\n🎯 Step 4: Update trending risk levels');
      const riskResults = await this.updateTrendingRiskLevels(processDate);
      
      // Step 5: Update account summary metrics
      console.log('\n🔄 Step 5: Update account summary metrics');
      await this.updateAccountSummaryMetrics();
      
      const endTime = Date.now();
      const duration = ((endTime - startTime) / 1000).toFixed(2);
      
      console.log(`\n✅ Production Daily ETL Pipeline completed successfully!`);
      console.log(`⏱️  Total duration: ${duration} seconds`);
      if (monthEndResults.historicalCalculationRan) {
        console.log(`📅 Month-end: Completed historical risk levels for ${monthEndResults.completedMonth}`);
      }
      console.log(`👥 Accounts refreshed: ${accountsResults.accountsUpdated}`);
      console.log(`📊 Processed ${extractResults.totalAccounts} accounts`);
      console.log(`📈 Updated ${monthlyResults.monthsUpdated} monthly records`);
      console.log(`🎯 Recalculated ${riskResults.accountsUpdated} trending risk levels`);
      
      return {
        success: true,
        processDate,
        duration: parseFloat(duration),
        monthEndResults,
        accountsResults,
        extractResults,
        monthlyResults,
        riskResults
      };
      
    } catch (error) {
      console.error(`❌ Production Daily ETL Pipeline failed:`, error);
      throw error;
    }
  }

  async checkAndCompleteMonthEnd(processDate) {
    const currentMonth = processDate.substring(0, 7); // YYYY-MM
    const dayOfMonth = parseInt(processDate.substring(8, 10));
    
    // Check if this is the first few days of a new month (days 1-3)
    // This handles timezone variations and ensures we don't miss month-end
    const isEarlyMonth = dayOfMonth <= 3;
    
    if (!isEarlyMonth) {
      console.log(`📅 Month-end check: Day ${dayOfMonth} of ${currentMonth} - no month-end processing needed`);
      return { historicalCalculationRan: false };
    }
    
    const previousMonth = this.getPreviousMonth(currentMonth);
    
    console.log(`📅 Month-end check: Processing ${processDate} (day ${dayOfMonth})`);
    console.log(`📅 Checking if ${previousMonth} needs historical risk level completion...`);
    
    const db = await this.getDatabase();
    
    try {
      // Check if previous month already has historical risk levels calculated
      const existingHistorical = await db.get(`
        SELECT COUNT(*) as count 
        FROM monthly_metrics 
        WHERE month = ? AND historical_risk_level IS NOT NULL
      `, [previousMonth]);
      
      if (existingHistorical.count > 0) {
        console.log(`✅ Historical risk levels already calculated for ${previousMonth}`);
        await db.close();
        return { historicalCalculationRan: false };
      }
      
      // Mark the previous month as complete
      await db.run(`
        UPDATE monthly_metrics 
        SET month_status = 'complete'
        WHERE month = ?
      `, [previousMonth]);
      
      console.log(`📅 Marked ${previousMonth} as complete, running historical risk calculation...`);
      
      // Run historical risk level calculation for the completed month
      const historicalPopulator = new HistoricalRiskPopulator();
      await historicalPopulator.populateHistoricalRiskLevels();
      
      console.log(`✅ Historical risk levels calculated for completed month: ${previousMonth}`);
      
      await db.close();
      
      return { 
        historicalCalculationRan: true,
        completedMonth: previousMonth
      };
      
    } catch (error) {
      await db.close();
      console.error(`❌ Month-end processing failed:`, error);
      // Don't throw - this shouldn't stop the daily ETL
      return { historicalCalculationRan: false, error: error.message };
    }
  }

  async updateAccountsTable() {
    console.log(`👥 Refreshing accounts table from BigQuery...`);
    
    try {
      // Use the existing accounts ETL method that already works
      const result = await this.accountsETL.populateAccounts();
      
      console.log(`✅ Accounts table refreshed using existing ETL`);
      
      return {
        accountsProcessed: result.accountsProcessed || 0,
        accountsUpdated: result.accountsUpdated || 0
      };
      
    } catch (error) {
      console.error(`❌ Accounts table refresh failed:`, error);
      throw error;
    }
  }

  async extractAndLoadDailyMetrics(processDate) {
    console.log(`📊 Extracting BigQuery data for ${processDate}...`);
    
    try {
      // Run all ETL scripts in parallel for maximum efficiency
      const [spendResult, textsResult, couponsResult, subsResult] = await Promise.all([
        this.spendETL.processDate(processDate).catch(err => {
          console.warn(`⚠️  Spend ETL warning for ${processDate}:`, err.message);
          return { updatedCount: 0, createdCount: 0, error: err.message };
        }),
        this.textsETL.processDate(processDate).catch(err => {
          console.warn(`⚠️  Texts ETL warning for ${processDate}:`, err.message);
          return { updatedCount: 0, createdCount: 0, error: err.message };
        }),
        this.couponsETL.processDate(processDate).catch(err => {
          console.warn(`⚠️  Coupons ETL warning for ${processDate}:`, err.message);
          return { updatedCount: 0, createdCount: 0, error: err.message };
        }),
        this.subsETL.processDate(processDate).catch(err => {
          console.warn(`⚠️  Subs ETL warning for ${processDate}:`, err.message);
          return { updatedCount: 0, createdCount: 0, error: err.message };
        })
      ]);

      const totalUpdated = (spendResult.updatedCount || 0) + (textsResult.updatedCount || 0) + 
                          (couponsResult.updatedCount || 0) + (subsResult.updatedCount || 0);
      const totalCreated = (spendResult.createdCount || 0) + (textsResult.createdCount || 0) + 
                          (couponsResult.createdCount || 0) + (subsResult.createdCount || 0);

      console.log(`✅ BigQuery extraction completed:`);
      console.log(`   - Spend: ${spendResult.updatedCount || 0} updated, ${spendResult.createdCount || 0} created`);
      console.log(`   - Texts: ${textsResult.updatedCount || 0} updated, ${textsResult.createdCount || 0} created`);
      console.log(`   - Coupons: ${couponsResult.updatedCount || 0} updated, ${couponsResult.createdCount || 0} created`);
      console.log(`   - Subs: ${subsResult.updatedCount || 0} updated, ${subsResult.createdCount || 0} created`);
      console.log(`📊 Total: ${totalUpdated} updated, ${totalCreated} created`);

      return {
        totalAccounts: Math.max(totalUpdated, totalCreated),
        totalUpdated,
        totalCreated,
        spendResult,
        textsResult,
        couponsResult,
        subsResult
      };

    } catch (error) {
      console.error(`❌ BigQuery extraction failed:`, error);
      throw error;
    }
  }

  async aggregateToMonthlyMetrics(processDate) {
    const month = processDate.substring(0, 7); // YYYY-MM format
    const monthLabel = this.formatMonthLabel(month); // Dynamic generation - no hardcoding!
    const currentMonth = this.getCurrentMonth();
    const monthStatus = (month === currentMonth) ? 'current' : 'complete';
    
    console.log(`📈 Aggregating monthly metrics for ${month} (${monthLabel}) [${monthStatus}]...`);
    
    const db = await this.getDatabase();
    
    try {
      // Full MTD recalculation approach (Tyler's preferred method)
      // This ensures data integrity and self-healing capabilities
      const result = await db.run(`
        INSERT OR REPLACE INTO monthly_metrics (
          account_id, 
          month, 
          month_label,
          total_spend, 
          total_texts_delivered, 
          total_coupons_redeemed, 
          avg_active_subs_cnt,
          days_with_activity,
          month_status,
          last_updated
        )
        SELECT 
          dm.account_id,
          ? as month,
          ? as month_label,
          COALESCE(SUM(dm.total_spend), 0) as total_spend,
          COALESCE(SUM(dm.total_texts_delivered), 0) as total_texts_delivered,
          COALESCE(SUM(dm.coupons_redeemed), 0) as total_coupons_redeemed,
          COALESCE(AVG(dm.active_subs_cnt), 0) as avg_active_subs_cnt,
          COUNT(DISTINCT dm.date) as days_with_activity,
          ? as month_status,
          datetime('now') as last_updated
        FROM daily_metrics dm
        INNER JOIN accounts a ON dm.account_id = a.account_id
        WHERE dm.date LIKE ? || '%'
          AND ? >= strftime('%Y-%m', a.launched_at)
        GROUP BY dm.account_id
      `, [month, monthLabel, monthStatus, month, month]);

      const monthsUpdated = result.changes || 0;
      
      console.log(`✅ Monthly metrics aggregation completed:`);
      console.log(`   - Month: ${monthLabel}`);
      console.log(`   - Records updated: ${monthsUpdated}`);
      
      await db.close();
      
      return {
        month,
        monthLabel,
        monthsUpdated
      };
      
    } catch (error) {
      await db.close();
      console.error(`❌ Monthly metrics aggregation failed:`, error);
      throw error;
    }
  }

  async updateTrendingRiskLevels(processDate) {
    const currentMonth = processDate.substring(0, 7);
    const dayOfMonth = parseInt(processDate.substring(8, 10));
    
    console.log(`🎯 Updating trending risk levels for ${currentMonth} (day ${dayOfMonth})...`);
    
    const db = await this.getDatabase();
    
    try {
      // Get all accounts with monthly metrics for current month
      // Include LAUNCHED, FROZEN, and accounts archived during the current month
      const accounts = await db.all(`
        SELECT 
          mm.*,
          a.launched_at,
          a.status,
          a.account_name,
          a.archived_at,
          a.earliest_unit_archived_at
        FROM monthly_metrics mm
        JOIN accounts a ON mm.account_id = a.account_id
        WHERE mm.month = ?
        AND (
          a.status IN ('LAUNCHED', 'FROZEN') OR 
          (a.status = 'ARCHIVED' AND 
           (COALESCE(a.archived_at, a.earliest_unit_archived_at) LIKE ? || '%'))
        )
      `, [currentMonth, currentMonth]);

      console.log(`📊 Processing ${accounts.length} accounts for risk assessment...`);
      
      let accountsUpdated = 0;
      
      for (const account of accounts) {
        // Calculate trending risk level using proportional analysis
        const riskLevel = this.calculateTrendingRiskLevel(account, dayOfMonth);
        
        // Update historical_risk_level in monthly_metrics table
        await db.run(`
          UPDATE monthly_metrics 
          SET historical_risk_level = ?, last_updated = datetime('now')
          WHERE account_id = ? AND month = ?
        `, [riskLevel, account.account_id, currentMonth]);
        
        // Also update accounts table for quick dashboard access
        await db.run(`
          UPDATE accounts 
          SET risk_level = ?, last_updated = datetime('now')
          WHERE account_id = ?
        `, [riskLevel, account.account_id]);
        
        accountsUpdated++;
      }
      
      console.log(`✅ Trending risk levels updated:`);
      console.log(`   - Accounts processed: ${accounts.length}`);
      console.log(`   - Risk levels updated: ${accountsUpdated}`);
      
      await db.close();
      
      return {
        accountsProcessed: accounts.length,
        accountsUpdated
      };
      
    } catch (error) {
      await db.close();
      console.error(`❌ Risk level update failed:`, error);
      throw error;
    }
  }


  getDaysInMonth(monthString) {
    const [year, month] = monthString.split('-').map(Number);
    return new Date(year, month, 0).getDate();
  }

  getPreviousMonth(monthString) {
    const [year, month] = monthString.split('-').map(Number);
    const prevMonth = month === 1 ? 12 : month - 1;
    const prevYear = month === 1 ? year - 1 : year;
    return `${prevYear}-${prevMonth.toString().padStart(2, '0')}`;
  }

  calculateMonthsSinceStart(launchedAt, currentMonth) {
    if (!launchedAt) return 0;
    
    const launchDate = new Date(launchedAt);
    const currentDate = new Date(currentMonth + '-01');
    
    const monthsDiff = (currentDate.getFullYear() - launchDate.getFullYear()) * 12 + 
                      (currentDate.getMonth() - launchDate.getMonth());
    
    return Math.max(0, monthsDiff);
  }

  calculateTrendingRiskLevel(monthlyData, dayOfMonth) {
    // Proportional Trending Analysis Logic
    const daysInMonth = this.getDaysInMonth(monthlyData.month);
    const progressPercentage = (dayOfMonth - 1) / daysInMonth;
    
    // Avoid division by zero for first day of month
    if (progressPercentage <= 0) {
      return 'low'; // Not enough data to trend
    }
    
    // Calculate projected month-end metrics
    const projectedRedemptions = monthlyData.total_coupons_redeemed / progressPercentage;
    const projectedSpend = monthlyData.total_spend / progressPercentage;
    
    // Apply trending thresholds to projected values
    // These are the same thresholds used in historical calculations but applied to projections
    if (projectedRedemptions < this.MONTHLY_REDEMPTIONS_THRESHOLD) {
      return 'high'; // Trending toward low redemptions
    }
    
    if (monthlyData.avg_active_subs_cnt < this.LOW_ACTIVITY_SUBS_THRESHOLD && 
        projectedRedemptions < this.LOW_ENGAGEMENT_COMBO_REDEMPTIONS_THRESHOLD) {
      return 'high'; // Low engagement combo trending
    }
    
    if (projectedRedemptions < 25 || projectedSpend < 50) {
      return 'medium'; // Moderate risk trending
    }
    
    return 'low'; // Trending well
  }

  async updateAccountSummaryMetrics() {
    console.log(`🔄 Updating account summary metrics...`);
    
    const db = await this.getDatabase();
    
    try {
      // Update accounts table with current month metrics for quick dashboard access
      const currentMonth = this.getCurrentMonth();
      
      await db.run(`
        UPDATE accounts 
        SET 
          total_spend = COALESCE((
            SELECT mm.total_spend 
            FROM monthly_metrics mm 
            WHERE mm.account_id = accounts.account_id 
            AND mm.month = ?
          ), 0),
          total_texts_delivered = COALESCE((
            SELECT mm.total_texts_delivered 
            FROM monthly_metrics mm 
            WHERE mm.account_id = accounts.account_id 
            AND mm.month = ?
          ), 0),
          coupons_redeemed = COALESCE((
            SELECT mm.total_coupons_redeemed 
            FROM monthly_metrics mm 
            WHERE mm.account_id = accounts.account_id 
            AND mm.month = ?
          ), 0),
          active_subs_cnt = COALESCE((
            SELECT mm.avg_active_subs_cnt 
            FROM monthly_metrics mm 
            WHERE mm.account_id = accounts.account_id 
            AND mm.month = ?
          ), 0),
          last_updated = datetime('now')
      `, [currentMonth, currentMonth, currentMonth, currentMonth]);

      console.log(`✅ Account summary metrics updated for ${currentMonth}`);
      
      await db.close();
      
    } catch (error) {
      await db.close();
      console.error(`❌ Account summary update failed:`, error);
      throw error;
    }
  }

  // Manual trigger method for testing
  async runManual(targetDate) {
    console.log(`🚀 Manual trigger: Running daily ETL for ${targetDate || 'yesterday'}`);
    return await this.runDailyPipeline(targetDate);
  }
}

// Allow direct script execution for testing
if (import.meta.url === `file://${process.argv[1]}`) {
  const etl = new DailyProductionETL();
  const targetDate = process.argv[2]; // Optional date parameter
  
  etl.runManual(targetDate)
    .then(result => {
      console.log('\n🎉 Manual ETL completed successfully:', result);
      process.exit(0);
    })
    .catch(error => {
      console.error('\n💥 Manual ETL failed:', error);
      process.exit(1);
    });
}