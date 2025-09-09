import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

class HistoricalRiskPopulator {
  constructor() {
    this.dbPath = process.env.SQLITE_DB_PATH || './churnguard_simulation.db';
    
    // Risk calculation thresholds
    this.MONTHLY_REDEMPTIONS_THRESHOLD = 10;
    this.LOW_ENGAGEMENT_COMBO_SUBS_THRESHOLD = 300;
    this.LOW_ENGAGEMENT_COMBO_REDEMPTIONS_THRESHOLD = 35;
    this.LOW_ACTIVITY_SUBS_THRESHOLD = 300; // Simplified from per-location logic
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

  calculateMonthsSinceStart(launchedAt, currentMonth) {
    if (!launchedAt) return 0;
    
    const launchDate = new Date(launchedAt);
    const currentDate = new Date(currentMonth + '-01');
    
    const monthsDiff = (currentDate.getFullYear() - launchDate.getFullYear()) * 12 + 
                      (currentDate.getMonth() - launchDate.getMonth());
    
    return Math.max(0, monthsDiff);
  }

  calculateRiskLevel(monthData, accountData, previousMonthData = null) {
    // Check if account was archived during this specific month (regardless of current status)
    const monthStart = new Date(monthData.month + '-01');
    const monthEnd = new Date(monthStart.getFullYear(), monthStart.getMonth() + 1, 0);
    
    const archivedDate = accountData.archived_at 
      ? new Date(accountData.archived_at)
      : accountData.earliest_unit_archived_at 
        ? new Date(accountData.earliest_unit_archived_at)
        : null;
    
    // If archived during this specific month = high risk
    if (archivedDate && archivedDate >= monthStart && archivedDate <= monthEnd) {
      return 'high';
    }

    // FROZEN accounts logic
    if (accountData.status === 'FROZEN') {
      const hasCurrentMonthTexts = monthData.total_texts_delivered > 0;
      return hasCurrentMonthTexts ? 'medium' : 'high';
    }

    // LAUNCHED/ACTIVE accounts: Flag-based system
    let flagCount = 0;
    const monthsSinceStart = this.calculateMonthsSinceStart(accountData.launched_at, monthData.month);
    
    // Flag 1: Monthly Redemptions (< 10 redemptions) - 1 point
    if (monthData.total_coupons_redeemed < this.MONTHLY_REDEMPTIONS_THRESHOLD) {
      flagCount++;
    }
    
    // Flag 2: Low Engagement Combo (< 300 subs AND < 35 redemptions) - 2 points
    // Only available for accounts after their first two months
    if (monthsSinceStart > 2) {
      if (monthData.avg_active_subs_cnt < this.LOW_ENGAGEMENT_COMBO_SUBS_THRESHOLD && 
          monthData.total_coupons_redeemed < this.LOW_ENGAGEMENT_COMBO_REDEMPTIONS_THRESHOLD) {
        flagCount += 2;
      }
    }
    
    // Flag 3: Low Activity (< 300 subscribers per account) - 1 point
    if (monthData.avg_active_subs_cnt < this.LOW_ACTIVITY_SUBS_THRESHOLD) {
      flagCount++;
    }
    
    // Flag 4 & 5: Drop flags only available after month 3 with previous month data
    if (previousMonthData && monthsSinceStart >= 3) {
      // Flag 4: Spend Drop (≥ 40% decrease) - 1 point
      if (previousMonthData.total_spend > 0) {
        const spendDrop = Math.max(0, (previousMonthData.total_spend - monthData.total_spend) / previousMonthData.total_spend);
        if (spendDrop >= this.SPEND_DROP_THRESHOLD) {
          flagCount++;
        }
      }
      
      // Flag 5: Redemptions Drop (≥ 50% decrease) - 1 point
      if (previousMonthData.total_coupons_redeemed > 0) {
        const redemptionsDrop = Math.max(0, (previousMonthData.total_coupons_redeemed - monthData.total_coupons_redeemed) / previousMonthData.total_coupons_redeemed);
        if (redemptionsDrop >= this.REDEMPTIONS_DROP_THRESHOLD) {
          flagCount++;
        }
      }
    }
    
    // Determine risk level based on flag count
    if (flagCount >= 3) return 'high';
    if (flagCount >= 1) return 'medium';
    return 'low';
  }

  async populateHistoricalRiskLevels() {
    console.log('🎯 Populating historical risk levels with correct flag logic...');
    const db = await this.getDatabase();

    try {
      // Get all monthly metrics with account data, ordered by account and month
      const monthlyData = await db.all(`
        SELECT 
          mm.*,
          a.status,
          a.archived_at,
          a.earliest_unit_archived_at,
          a.launched_at
        FROM monthly_metrics mm
        INNER JOIN accounts a ON mm.account_id = a.account_id
        ORDER BY mm.account_id, mm.month
      `);

      console.log(`📊 Processing ${monthlyData.length} monthly records...`);

      let updateCount = 0;
      let accountData = {};
      
      // Group data by account for easier previous month lookup
      for (const record of monthlyData) {
        if (!accountData[record.account_id]) {
          accountData[record.account_id] = [];
        }
        accountData[record.account_id].push(record);
      }
      
      // Process each account's monthly records
      for (const [accountId, records] of Object.entries(accountData)) {
        for (let i = 0; i < records.length; i++) {
          const currentRecord = records[i];
          const previousRecord = i > 0 ? records[i - 1] : null;
          
          const riskLevel = this.calculateRiskLevel(
            currentRecord, 
            currentRecord, 
            previousRecord
          );
          
          // Update the record
          await db.run(`
            UPDATE monthly_metrics 
            SET historical_risk_level = ?
            WHERE account_id = ? AND month = ?
          `, [riskLevel, currentRecord.account_id, currentRecord.month]);
          
          updateCount++;
          
          if (updateCount % 1000 === 0) {
            console.log(`   ✅ Updated ${updateCount} records...`);
          }
        }
      }

      // Verify the results
      const riskDistribution = await db.all(`
        SELECT 
          historical_risk_level,
          COUNT(*) as count
        FROM monthly_metrics 
        WHERE historical_risk_level IS NOT NULL
        GROUP BY historical_risk_level
        ORDER BY historical_risk_level
      `);

      console.log(`\\n📊 Historical Risk Distribution:`);
      riskDistribution.forEach(row => {
        console.log(`   ${row.historical_risk_level}: ${row.count.toLocaleString()} records`);
      });

      // Sample validation - show some risk calculations
      const sampleData = await db.all(`
        SELECT 
          mm.account_id,
          a.account_name,
          mm.month,
          mm.historical_risk_level,
          mm.total_coupons_redeemed,
          mm.avg_active_subs_cnt,
          mm.total_spend,
          a.status,
          a.launched_at
        FROM monthly_metrics mm
        INNER JOIN accounts a ON mm.account_id = a.account_id
        WHERE mm.historical_risk_level IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 5
      `);

      console.log(`\\n🔍 Sample Risk Calculations:`);
      sampleData.forEach(row => {
        console.log(`   ${row.account_name} (${row.month}): ${row.historical_risk_level.toUpperCase()} risk`);
        console.log(`     Status: ${row.status}, Subs: ${row.avg_active_subs_cnt}, Redemptions: ${row.total_coupons_redeemed}, Spend: $${row.total_spend}`);
      });

      console.log(`\\n✅ Successfully populated ${updateCount} historical risk levels!`);
      
    } finally {
      await db.close();
    }
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const populator = new HistoricalRiskPopulator();
  populator.populateHistoricalRiskLevels()
    .then(() => {
      console.log('🎉 Historical risk level population completed!');
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Failed to populate historical risk levels:', error);
      process.exit(1);
    });
}

export { HistoricalRiskPopulator };