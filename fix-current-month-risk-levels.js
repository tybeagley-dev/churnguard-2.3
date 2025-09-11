import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

class CurrentMonthRiskFixer {
  constructor() {
    this.dbPath = process.env.SQLITE_DB_PATH || './data/churnguard_simulation.db';
    
    // Risk calculation thresholds for trending logic
    this.MONTHLY_REDEMPTIONS_THRESHOLD = 3;
    this.LOW_ACTIVITY_SUBSCRIBERS_THRESHOLD = 300;
    this.LOW_ACTIVITY_REDEMPTIONS_THRESHOLD = 35;
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

  // Calculate trending risk level using projected end-of-month values
  calculateTrendingRisk(account, currentMonthData, previousMonthData) {
    // Only calculate trending risk for current month view
    const today = new Date();
    const currentDay = today.getDate();
    const daysInMonth = new Date(today.getFullYear(), today.getMonth() + 1, 0).getDate();
    
    // Progress through the month (exclude today since it's incomplete)
    const progressPercentage = Math.max(0.1, (currentDay - 1) / daysInMonth);
    
    // Project end-of-month values
    const projectedSpend = (currentMonthData.total_spend || 0) / progressPercentage;
    const projectedRedemptions = (currentMonthData.total_coupons_redeemed || 0) / progressPercentage;
    
    // Get previous month data for comparison
    const previousMonthSpend = previousMonthData ? previousMonthData.total_spend || 0 : 0;
    const previousMonthRedemptions = previousMonthData ? previousMonthData.total_coupons_redeemed || 0 : 0;
    
    // Calculate projected drops
    const projectedSpendDrop = previousMonthSpend > 0 
      ? Math.max(0, (previousMonthSpend - projectedSpend) / previousMonthSpend) 
      : 0;
    const projectedRedemptionsDrop = previousMonthRedemptions > 0 
      ? Math.max(0, (previousMonthRedemptions - projectedRedemptions) / previousMonthRedemptions) 
      : 0;
    
    // Calculate individual flags for projected end-of-month scenario
    const monthlyRedemptionsFlag = projectedRedemptions <= this.MONTHLY_REDEMPTIONS_THRESHOLD;
    const lowActivityFlag = (currentMonthData.avg_active_subs_cnt || 0) < this.LOW_ACTIVITY_SUBSCRIBERS_THRESHOLD && 
                           projectedRedemptions < this.LOW_ACTIVITY_REDEMPTIONS_THRESHOLD;
    const spendDropFlag = projectedSpendDrop >= this.SPEND_DROP_THRESHOLD;
    const redemptionsDropFlag = projectedRedemptionsDrop >= this.REDEMPTIONS_DROP_THRESHOLD;
    
    // Count flags
    let flagCount = 0;
    if (monthlyRedemptionsFlag) flagCount++;
    if (lowActivityFlag) flagCount++;
    if (spendDropFlag) flagCount++;
    if (redemptionsDropFlag) flagCount++;
    
    // Determine trending risk level
    let trending_risk_level;
    if (flagCount === 0) trending_risk_level = 'low';
    else if (flagCount >= 1 && flagCount <= 2) trending_risk_level = 'medium';
    else trending_risk_level = 'high'; // 3-4 flags
    
    return trending_risk_level;
  }

  async fixCurrentMonthRiskLevels() {
    console.log('🔧 Fixing current month risk levels with trending logic...');
    const db = await this.getDatabase();

    try {
      // Get current month (September 2025)
      const currentMonth = '2025-09';
      const previousMonth = '2025-08';
      
      console.log(`📅 Processing ${currentMonth} with trending logic...`);
      
      // Get all current month records with account info
      const currentMonthAccounts = await db.all(`
        SELECT 
          mm.*,
          a.status,
          a.account_name
        FROM monthly_metrics mm
        INNER JOIN accounts a ON mm.account_id = a.account_id
        WHERE mm.month = ? AND mm.month_status = 'current'
      `, [currentMonth]);

      // Get previous month data for comparison
      const previousMonthData = await db.all(`
        SELECT *
        FROM monthly_metrics
        WHERE month = ?
      `, [previousMonth]);
      
      // Create lookup for previous month data
      const previousMonthLookup = {};
      previousMonthData.forEach(row => {
        previousMonthLookup[row.account_id] = row;
      });

      console.log(`📊 Processing ${currentMonthAccounts.length} current month accounts...`);

      let updateCount = 0;
      
      for (const account of currentMonthAccounts) {
        let riskLevel;
        
        // FROZEN accounts special handling
        if (account.status === 'FROZEN') {
          riskLevel = 'high'; // Simple approach: all FROZEN = high risk in current month
        } else {
          // Calculate trending risk for non-FROZEN accounts
          const previousData = previousMonthLookup[account.account_id];
          riskLevel = this.calculateTrendingRisk(account, account, previousData);
        }
        
        // Update the record
        await db.run(`
          UPDATE monthly_metrics 
          SET historical_risk_level = ?
          WHERE account_id = ? AND month = ?
        `, [riskLevel, account.account_id, account.month]);
        
        updateCount++;
        
        if (updateCount % 100 === 0) {
          console.log(`   ✅ Updated ${updateCount} current month records...`);
        }
      }

      // Verify the new results
      const newRiskDistribution = await db.all(`
        SELECT 
          historical_risk_level,
          COUNT(*) as count
        FROM monthly_metrics 
        WHERE month = ? AND month_status = 'current'
        GROUP BY historical_risk_level
        ORDER BY historical_risk_level
      `, [currentMonth]);

      console.log(`\n📊 Updated ${currentMonth} Risk Distribution:`);
      newRiskDistribution.forEach(row => {
        console.log(`   ${row.historical_risk_level}: ${row.count.toLocaleString()} accounts`);
      });

      console.log(`\n✅ Successfully updated ${updateCount} current month risk levels with trending logic!`);
      
    } finally {
      await db.close();
    }
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const fixer = new CurrentMonthRiskFixer();
  fixer.fixCurrentMonthRiskLevels()
    .then(() => {
      console.log('🎉 Current month risk level fix completed!');
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Failed to fix current month risk levels:', error);
      process.exit(1);
    });
}

export { CurrentMonthRiskFixer };