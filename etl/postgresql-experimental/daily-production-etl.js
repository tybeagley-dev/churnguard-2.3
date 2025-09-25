import { DailySpendETLSQLite } from './daily-spend-etl-sqlite.js';
import { DailyTextsETLSQLite } from './daily-texts-etl-sqlite.js';
import { DailyCouponsETLSQLite } from './daily-coupons-etl-sqlite.js';
import { DailySubsETLSQLite } from './daily-subs-etl-sqlite.js';
import { AccountsETLSQLite } from './accounts-etl-sqlite.js';
import { HistoricalRiskPopulator } from './populate-historical-risk-levels.js';
import { HubSpotSyncService } from '../src/services/hubspot-sync.js';
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
    this.hubspotSync = new HubSpotSyncService();
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

  getMonthEnd(yearMonth) {
    const [year, month] = yearMonth.split('-');
    const lastDay = new Date(parseInt(year), parseInt(month), 0).getDate();
    return `${yearMonth}-${lastDay.toString().padStart(2, '0')}`;
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

      // Step 6: Sync to HubSpot (if API key is configured)
      let hubspotResults = null;
      if (process.env.HUBSPOT_API_KEY) {
        console.log('\n📤 Step 6: Sync risk data to HubSpot');
        try {
          hubspotResults = await this.hubspotSync.syncAccountsToHubSpot(processDate, 'daily');
        } catch (error) {
          console.warn(`⚠️  HubSpot sync failed (continuing ETL): ${error.message}`);
          hubspotResults = { success: false, error: error.message };
        }
      } else {
        console.log('\n⏭️  Step 6: Skipping HubSpot sync (HUBSPOT_API_KEY not configured)');
      }

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
      if (hubspotResults && hubspotResults.success) {
        console.log(`📤 HubSpot sync: ${hubspotResults.successfulSyncs}/${hubspotResults.totalAccounts} accounts synced`);
      }

      return {
        success: true,
        processDate,
        duration: parseFloat(duration),
        monthEndResults,
        accountsResults,
        extractResults,
        monthlyResults,
        riskResults,
        hubspotResults
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
      
      // Run historical risk calculation for the completed previous month
      console.log(`📅 Running historical risk calculation for completed month ${previousMonth}...`);
      
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
          last_updated,
          historical_risk_level,
          risk_reasons,
          trending_risk_reasons,
          trending_risk_level
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
          datetime('now') as last_updated,
          CASE
            WHEN ? = strftime('%Y-%m', 'now') THEN NULL
            ELSE (SELECT historical_risk_level FROM monthly_metrics WHERE account_id = dm.account_id AND month = ? LIMIT 1)
          END as historical_risk_level,
          CASE
            WHEN ? = strftime('%Y-%m', 'now') THEN NULL
            ELSE (SELECT risk_reasons FROM monthly_metrics WHERE account_id = dm.account_id AND month = ? LIMIT 1)
          END as risk_reasons,
          NULL as trending_risk_reasons,
          NULL as trending_risk_level
        FROM daily_metrics dm
        INNER JOIN accounts a ON dm.account_id = a.account_id
        WHERE dm.date LIKE ? || '%'
          AND ? >= strftime('%Y-%m', a.launched_at)
        GROUP BY dm.account_id
      `, [month, monthLabel, month, month, month, month, month, month]);

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
      // Get all accounts with monthly metrics for current month using proper eligibility criteria
      // Same criteria as Monthly Trends query
      const monthEnd = this.getMonthEnd(currentMonth); // e.g., '2025-09-30'
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
          a.launched_at IS NOT NULL
          AND a.launched_at <= ? || ' 23:59:59'
          AND (
            COALESCE(a.archived_at, a.earliest_unit_archived_at) IS NULL
            OR COALESCE(a.archived_at, a.earliest_unit_archived_at) >= ? || '-01'
          )
        )
      `, [currentMonth, monthEnd, currentMonth]);

      // Get previous month for drop calculations
      const previousMonth = this.getPreviousMonth(currentMonth);
      
      console.log(`📊 Processing ${accounts.length} accounts for risk assessment...`);
      
      let accountsUpdated = 0;
      
      for (const account of accounts) {
        // Get same-day totals from previous month for proper drop calculations
        let previousMonthSameDayData = null;
        try {
          const previousMonthSameDay = `${previousMonth}-${dayOfMonth.toString().padStart(2, '0')}`;
          
          // Sum daily metrics up to same day of previous month
          const prevMonthTotals = await db.get(`
            SELECT 
              SUM(total_spend) as total_spend,
              SUM(coupons_redeemed) as total_coupons_redeemed,
              AVG(active_subs_cnt) as avg_active_subs_cnt,
              SUM(total_texts_delivered) as total_texts_delivered
            FROM daily_metrics
            WHERE account_id = ? 
              AND date >= ? 
              AND date <= ?
          `, [
            account.account_id, 
            `${previousMonth}-01`,
            previousMonthSameDay
          ]);
          
          if (prevMonthTotals && prevMonthTotals.total_spend !== null) {
            previousMonthSameDayData = {
              total_spend: prevMonthTotals.total_spend || 0,
              total_coupons_redeemed: prevMonthTotals.total_coupons_redeemed || 0,
              avg_active_subs_cnt: prevMonthTotals.avg_active_subs_cnt || 0,
              total_texts_delivered: prevMonthTotals.total_texts_delivered || 0
            };
          }
        } catch (error) {
          // Previous month same-day data not available - this is normal for new accounts
          previousMonthSameDayData = null;
        }
        
        // Calculate trending risk level using the full 8-flag system with proper comparisons
        const riskResult = this.calculateTrendingRiskLevel(account, dayOfMonth, account, previousMonthSameDayData);
        
        // Update trending_risk_level and trending_risk_reasons in monthly_metrics table
        await db.run(`
          UPDATE monthly_metrics 
          SET trending_risk_level = ?, trending_risk_reasons = ?, last_updated = datetime('now')
          WHERE account_id = ? AND month = ?
        `, [riskResult.level, JSON.stringify(riskResult.reasons), account.account_id, currentMonth]);
        
        // Also update accounts table for quick dashboard access
        await db.run(`
          UPDATE accounts 
          SET risk_level = ?, last_updated = datetime('now')
          WHERE account_id = ?
        `, [riskResult.level, account.account_id]);
        
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

  calculateTrendingRiskLevel(monthlyData, dayOfMonth, accountData, previousMonthData = null) {
    const reasons = [];
    
    // Use the same 8-flag system as historical calculations but with projected values
    
    // Check if account was archived during this specific month (regardless of current status)
    const [year, month] = monthlyData.month.split('-');
    const monthStart = new Date(parseInt(year), parseInt(month) - 1, 1);
    const monthEnd = new Date(parseInt(year), parseInt(month), 0);
    monthEnd.setHours(23, 59, 59, 999);
    
    const archivedDate = accountData.archived_at 
      ? new Date(accountData.archived_at)
      : accountData.earliest_unit_archived_at 
        ? new Date(accountData.earliest_unit_archived_at)
        : null;
    
    // Flag 1: If archived during this specific month = high risk
    if (archivedDate && archivedDate >= monthStart && archivedDate <= monthEnd) {
      reasons.push('Recently Archived');
      return { level: 'high', reasons };
    }
    
    // Flags 2-3: FROZEN accounts logic
    if (accountData.status === 'FROZEN') {
      const hasCurrentMonthTexts = monthlyData.total_texts_delivered > 0;
      reasons.push('Frozen Account Status');
      
      // Check if it's been 1+ month since last text (Frozen & Inactive)
      const isFrozenAndInactive = !hasCurrentMonthTexts;
      
      if (isFrozenAndInactive) {
        reasons.push('Frozen & Inactive');
        return { level: 'high', reasons };
      }
      
      return { level: 'medium', reasons };
    }
    
    // Flags 4-8: LAUNCHED/ACTIVE accounts: Flag-based system with projected values
    const daysInMonth = this.getDaysInMonth(monthlyData.month);
    const progressPercentage = (dayOfMonth - 1) / daysInMonth;
    
    // Avoid division by zero for first day of month
    if (progressPercentage <= 0) {
      reasons.push('No flags');
      return { level: 'low', reasons }; // Not enough data to trend
    }
    
    // Calculate proportional thresholds based on progress through month
    const proportionalRedemptionsThreshold = this.MONTHLY_REDEMPTIONS_THRESHOLD * progressPercentage;
    const proportionalLowEngagementRedemptionsThreshold = this.LOW_ENGAGEMENT_COMBO_REDEMPTIONS_THRESHOLD * progressPercentage;
    
    let flagCount = 0;
    const monthsSinceStart = this.calculateMonthsSinceStart(accountData.launched_at, monthlyData.month);
    
    // Flag 4: Monthly Redemptions (proportional to day of month) - 1 point
    if (monthlyData.total_coupons_redeemed < proportionalRedemptionsThreshold) {
      flagCount++;
      reasons.push('Low Monthly Redemptions');
    }
    
    // Flag 5: Low Engagement Combo (< 300 subs AND proportional redemptions) - 2 points
    // Only available for accounts after their first two months
    if (monthsSinceStart > 2) {
      if (monthlyData.avg_active_subs_cnt < this.LOW_ENGAGEMENT_COMBO_SUBS_THRESHOLD && 
          monthlyData.total_coupons_redeemed < proportionalLowEngagementRedemptionsThreshold) {
        flagCount += 2;
        reasons.push('Low Engagement Combo');
      }
    }
    
    // Flag 6: Low Activity (< 300 subscribers) - 1 point
    if (monthlyData.avg_active_subs_cnt < this.LOW_ACTIVITY_SUBS_THRESHOLD) {
      flagCount++;
      reasons.push('Low Activity');
    }
    
    // Flags 7 & 8: Drop flags using same-day comparisons (apples-to-apples)
    if (previousMonthData && monthsSinceStart >= 3) {
      // Flag 7: Spend Drop (≥ 40% decrease from same day of previous month) - 1 point
      if (previousMonthData.total_spend > 0) {
        const currentSpend = monthlyData.total_spend; // Actual current month-to-date spend
        const spendDrop = Math.max(0, (previousMonthData.total_spend - currentSpend) / previousMonthData.total_spend);
        if (spendDrop >= this.SPEND_DROP_THRESHOLD) {
          flagCount++;
          reasons.push('Spend Drop');
        }
      }
      
      // Flag 8: Redemptions Drop (≥ 50% decrease from same day of previous month) - 1 point
      if (previousMonthData.total_coupons_redeemed > 0) {
        const currentRedemptions = monthlyData.total_coupons_redeemed; // Actual current month-to-date redemptions
        const redemptionsDrop = Math.max(0, (previousMonthData.total_coupons_redeemed - currentRedemptions) / previousMonthData.total_coupons_redeemed);
        if (redemptionsDrop >= this.REDEMPTIONS_DROP_THRESHOLD) {
          flagCount++;
          reasons.push('Redemptions Drop');
        }
      }
    }
    
    // If no flags, add "No flags"
    if (reasons.length === 0) {
      reasons.push('No flags');
    }
    
    // Determine risk level based on flag count (same as historical)
    let level = 'low';
    if (flagCount >= 3) level = 'high';
    else if (flagCount >= 1) level = 'medium';
    
    return { level, reasons };
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