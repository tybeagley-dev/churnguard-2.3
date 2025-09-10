import { DailySpendETLSQLite } from './daily-spend-etl-sqlite.js';
import { DailyTextsETLSQLite } from './daily-texts-etl-sqlite.js';
import { DailyCouponsETLSQLite } from './daily-coupons-etl-sqlite.js';
import { DailySubsETLSQLite } from './daily-subs-etl-sqlite.js';
import { AccountsETLSQLite } from './accounts-etl-sqlite.js';
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
      // Step 0: Update accounts table from BigQuery
      console.log('\n👥 Step 0: Update accounts table from BigQuery');
      const accountsResults = await this.updateAccountsTable();
      
      // Step 1: Extract from BigQuery and Load to daily_metrics
      console.log('\n📊 Step 1: BigQuery Extract & Load to daily_metrics');
      const extractResults = await this.extractAndLoadDailyMetrics(processDate);
      
      // Step 2: Aggregate to monthly_metrics (full MTD recalculation)
      console.log('\n📈 Step 2: Aggregate to monthly_metrics');
      const monthlyResults = await this.aggregateToMonthlyMetrics(processDate);
      
      // Step 3: Update trending risk levels
      console.log('\n🎯 Step 3: Update trending risk levels');
      const riskResults = await this.updateTrendingRiskLevels(processDate);
      
      // Step 4: Update account summary metrics
      console.log('\n🔄 Step 4: Update account summary metrics');
      await this.updateAccountSummaryMetrics();
      
      const endTime = Date.now();
      const duration = ((endTime - startTime) / 1000).toFixed(2);
      
      console.log(`\n✅ Production Daily ETL Pipeline completed successfully!`);
      console.log(`⏱️  Total duration: ${duration} seconds`);
      console.log(`👥 Accounts refreshed: ${accountsResults.accountsUpdated}`);
      console.log(`📊 Processed ${extractResults.totalAccounts} accounts`);
      console.log(`📈 Updated ${monthlyResults.monthsUpdated} monthly records`);
      console.log(`🎯 Recalculated ${riskResults.accountsUpdated} risk levels`);
      
      return {
        success: true,
        processDate,
        duration: parseFloat(duration),
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
        WHERE dm.date LIKE ? || '%'
        GROUP BY dm.account_id
      `, [month, monthLabel, monthStatus, month]);

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
        // Pro-rate metrics based on days elapsed in month
        const proRatedMetrics = this.proRateMetrics(account, dayOfMonth);
        
        // Calculate risk level using existing logic
        const riskLevel = this.calculateTrendingRiskLevel(proRatedMetrics, account);
        
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

  proRateMetrics(monthlyData, dayOfMonth) {
    const daysInMonth = this.getDaysInMonth(monthlyData.month);
    const proRationFactor = daysInMonth / dayOfMonth;
    
    return {
      account_id: monthlyData.account_id,
      total_spend: monthlyData.total_spend * proRationFactor,
      total_texts_delivered: monthlyData.total_texts_delivered * proRationFactor,
      total_coupons_redeemed: monthlyData.total_coupons_redeemed * proRationFactor,
      avg_active_subs_cnt: monthlyData.avg_active_subs_cnt,
      days_with_activity: monthlyData.days_with_activity,
      month: monthlyData.month
    };
  }

  getDaysInMonth(monthString) {
    const [year, month] = monthString.split('-').map(Number);
    return new Date(year, month, 0).getDate();
  }

  calculateTrendingRiskLevel(proRatedMetrics, accountData) {
    // Use the existing risk calculation logic but with pro-rated metrics
    const monthData = {
      total_spend: proRatedMetrics.total_spend,
      total_texts_delivered: proRatedMetrics.total_texts_delivered,
      total_coupons_redeemed: proRatedMetrics.total_coupons_redeemed,
      avg_active_subs_cnt: proRatedMetrics.avg_active_subs_cnt
    };
    
    // Simplified risk calculation (matches existing logic in populate-historical-risk-levels.js)
    const MONTHLY_REDEMPTIONS_THRESHOLD = 10;
    const LOW_ENGAGEMENT_COMBO_SUBS_THRESHOLD = 300;
    const LOW_ENGAGEMENT_COMBO_REDEMPTIONS_THRESHOLD = 35;
    
    const redemptions = monthData.total_coupons_redeemed || 0;
    const subs = monthData.avg_active_subs_cnt || 0;
    const spend = monthData.total_spend || 0;
    
    // High risk conditions
    if (redemptions < MONTHLY_REDEMPTIONS_THRESHOLD) return 'high';
    if (subs > LOW_ENGAGEMENT_COMBO_SUBS_THRESHOLD && redemptions < LOW_ENGAGEMENT_COMBO_REDEMPTIONS_THRESHOLD) return 'high';
    if (spend < 20) return 'high';
    if (accountData.status === 'FROZEN') return 'high';
    
    // Medium risk conditions  
    if (redemptions < 25 || spend < 50) return 'medium';
    
    // Default to low risk
    return 'low';
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