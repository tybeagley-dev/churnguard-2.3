import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

class MonthlyRollupETLPostgresNative {
  constructor() {
    // Initialize PostgreSQL pool with proper SSL configuration
    if (!process.env.DATABASE_URL) {
      throw new Error('DATABASE_URL environment variable is required');
    }

    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: {
        rejectUnauthorized: false,
        sslmode: 'require'
      },
      max: 10,                    // Maximum connections in pool
      idleTimeoutMillis: 30000,   // Close idle connections after 30s
      connectionTimeoutMillis: 10000,  // Wait 10s for connection
      statement_timeout: 600000,  // 10 minute query timeout (monthly aggregation is heavy)
      query_timeout: 600000,      // 10 minute query timeout
      keepAlive: true,
      keepAliveInitialDelayMillis: 10000
    });

    // Risk calculation thresholds (IDENTICAL to SQLite version)
    this.MONTHLY_REDEMPTIONS_THRESHOLD = 10;
    this.LOW_ENGAGEMENT_COMBO_SUBS_THRESHOLD = 300;
    this.LOW_ENGAGEMENT_COMBO_REDEMPTIONS_THRESHOLD = 35;
    this.LOW_ACTIVITY_SUBS_THRESHOLD = 300;
    this.SPEND_DROP_THRESHOLD = 0.40; // 40%
    this.REDEMPTIONS_DROP_THRESHOLD = 0.50; // 50%

    // Test connection on startup
    this.pool.on('error', (err) => {
      console.error('❌ PostgreSQL pool error:', err);
    });
  }

  async testConnection() {
    const client = await this.pool.connect();
    try {
      const result = await client.query('SELECT NOW() as current_time');
      console.log(`✅ PostgreSQL connected at ${result.rows[0].current_time}`);
    } catch (error) {
      console.error('❌ PostgreSQL connection test failed:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  // Get month details in IDENTICAL format to SQLite version
  getMonthDetails(month = null) {
    const targetMonth = month || new Date().toISOString().slice(0, 7); // YYYY-MM format
    const monthLabel = new Date(targetMonth + '-01').toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      timeZone: 'UTC'
    });

    return { targetMonth, monthLabel };
  }

  async updateMonthlyMetrics(month = null) {
    const { targetMonth, monthLabel } = this.getMonthDetails(month);

    console.log(`🔄 Updating monthly metrics for ${monthLabel} (${targetMonth})...`);
    console.log(`⚠️  WARNING: This will DELETE and recreate ALL monthly_metrics for ${monthLabel}`);
    console.log(`⚠️  WARNING: This will WIPE risk_reasons, trending_risk_level, and historical_risk_level data!`);

    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      // Step 1: Delete existing monthly data for this month (IDENTICAL to SQLite logic)
      console.log(`🗑️  Deleting existing monthly_metrics for ${targetMonth}...`);
      const deleteResult = await client.query(
        'DELETE FROM monthly_metrics WHERE month = $1',
        [targetMonth]
      );
      console.log(`✅ Deleted ${deleteResult.rowCount} existing monthly metric rows`);

      // Step 2: Recreate monthly data from daily_metrics (IDENTICAL aggregation logic to SQLite)
      console.log(`📈 Aggregating daily metrics to monthly for ${targetMonth}...`);

      const insertResult = await client.query(`
        INSERT INTO monthly_metrics (
          account_id, month, month_label, total_spend, total_texts_delivered,
          total_coupons_redeemed, avg_active_subs_cnt, updated_at
        )
        SELECT
          a.account_id,
          $1::text as month,
          $2::text as month_label,
          SUM(dm.total_spend) as total_spend,
          SUM(dm.total_texts_delivered) as total_texts_delivered,
          SUM(dm.coupons_redeemed) as total_coupons_redeemed,
          ROUND(AVG(dm.active_subs_cnt)) as avg_active_subs_cnt,
          NOW() as updated_at
        FROM accounts a
        LEFT JOIN daily_metrics dm ON a.account_id = dm.account_id
          AND TO_CHAR(dm.date::date, 'YYYY-MM') = $3
        WHERE (
          -- Include accounts launched before or during the target month
          a.launched_at::date <= (($4 || '-01')::date + INTERVAL '1 month' - INTERVAL '1 day')::date
          AND (
            -- Account is not ARCHIVED status (include regardless of earliest_unit_archived_at)
            a.status != 'ARCHIVED'
            OR
            -- Account IS ARCHIVED and was archived after the start of the target month
            -- Use archived_at if it exists, otherwise fall back to earliest_unit_archived_at
            (a.status = 'ARCHIVED'
             AND COALESCE(a.archived_at, a.earliest_unit_archived_at)::date >= ($5 || '-01')::date)
          )
        )
        GROUP BY a.account_id
      `, [
        targetMonth,     // $1: month
        monthLabel,      // $2: month_label
        targetMonth,     // $3: date filter for daily_metrics
        targetMonth,     // $4: launched_at comparison
        targetMonth      // $5: archived_at comparison
      ]);

      console.log(`✅ Created ${insertResult.rowCount} monthly metric rows for ${monthLabel}`);

      // Step 3: Calculate trending risk levels for current month only
      const currentMonth = new Date().toISOString().slice(0, 7); // YYYY-MM format
      if (targetMonth === currentMonth) {
        console.log(`🎯 Calculating trending risk levels for current month ${monthLabel}...`);
        const trendingResult = await this.calculateTrendingRiskForMonth(client, targetMonth);
        console.log(`✅ Updated ${trendingResult} accounts with trending risk levels`);
      }

      await client.query('COMMIT');

      // IDENTICAL return format to SQLite version
      return {
        month: targetMonth,
        monthLabel: monthLabel,
        accountsProcessed: insertResult.rowCount
      };

    } catch (error) {
      await client.query('ROLLBACK');
      console.error(`❌ Monthly rollup ETL failed for ${targetMonth}:`, error);
      throw error;
    } finally {
      client.release();
    }
  }

  // Process a specific month or current month by default
  async processMonth(month = null) {
    await this.testConnection();
    const result = await this.updateMonthlyMetrics(month);
    return result;
  }

  // Helper methods for trending risk calculation
  calculateMonthsSinceStart(launchedAt, currentMonth) {
    if (!launchedAt) return 0;
    const launchDate = new Date(launchedAt);
    const currentDate = new Date(currentMonth + '-01');
    const monthsDiff = (currentDate.getFullYear() - launchDate.getFullYear()) * 12 +
                      (currentDate.getMonth() - launchDate.getMonth());
    return Math.max(0, monthsDiff);
  }

  getDaysInMonth(month) {
    const [year, monthNum] = month.split('-');
    return new Date(parseInt(year), parseInt(monthNum), 0).getDate();
  }

  async calculateTrendingRiskForMonth(client, targetMonth) {
    const today = new Date();
    const dayOfMonth = today.getDate();

    // Get all monthly metrics for current month with account details
    const accountsResult = await client.query(`
      SELECT
        mm.account_id, mm.month, mm.total_spend, mm.total_texts_delivered,
        mm.total_coupons_redeemed, mm.avg_active_subs_cnt,
        a.launched_at, a.status, a.archived_at, a.earliest_unit_archived_at
      FROM monthly_metrics mm
      JOIN accounts a ON mm.account_id = a.account_id
      WHERE mm.month = $1
      ORDER BY mm.account_id
    `, [targetMonth]);

    console.log(`📊 Processing ${accountsResult.rows.length} accounts for trending risk...`);

    // Get previous month for comparison
    const prevDate = new Date(targetMonth + '-01');
    prevDate.setMonth(prevDate.getMonth() - 1);
    const previousMonth = prevDate.toISOString().slice(0, 7);

    let accountsUpdated = 0;

    for (const account of accountsResult.rows) {
      // Get same-day totals from previous month for apples-to-apples comparison
      let previousMonthSameDayData = null;
      try {
        const previousMonthSameDay = `${previousMonth}-${dayOfMonth.toString().padStart(2, '0')}`;

        const prevResult = await client.query(`
          SELECT
            SUM(total_spend) as total_spend,
            SUM(coupons_redeemed) as total_coupons_redeemed,
            AVG(active_subs_cnt) as avg_active_subs_cnt,
            SUM(total_texts_delivered) as total_texts_delivered
          FROM daily_metrics
          WHERE account_id = $1
            AND date >= $2
            AND date <= $3
        `, [
          account.account_id,
          `${previousMonth}-01`,
          previousMonthSameDay
        ]);

        if (prevResult.rows[0] && prevResult.rows[0].total_spend !== null) {
          previousMonthSameDayData = {
            total_spend: parseFloat(prevResult.rows[0].total_spend) || 0,
            total_coupons_redeemed: parseInt(prevResult.rows[0].total_coupons_redeemed) || 0,
            avg_active_subs_cnt: parseFloat(prevResult.rows[0].avg_active_subs_cnt) || 0,
            total_texts_delivered: parseInt(prevResult.rows[0].total_texts_delivered) || 0
          };
        }
      } catch (error) {
        // Previous month same-day data not available - normal for new accounts
        previousMonthSameDayData = null;
      }

      // Calculate trending risk using the same algorithm as SQLite
      const riskResult = this.calculateTrendingRiskLevel(account, dayOfMonth, account, previousMonthSameDayData);

      // Update trending_risk_level and trending_risk_reasons
      await client.query(`
        UPDATE monthly_metrics
        SET trending_risk_level = $1, trending_risk_reasons = $2, updated_at = NOW()
        WHERE account_id = $3 AND month = $4
      `, [riskResult.level, JSON.stringify(riskResult.reasons), account.account_id, targetMonth]);

      accountsUpdated++;

      // Progress logging
      if (accountsUpdated % 100 === 0) {
        console.log(`   🎯 Updated ${accountsUpdated}/${accountsResult.rows.length} accounts...`);
      }
    }

    return accountsUpdated;
  }

  calculateTrendingRiskLevel(monthlyData, dayOfMonth, accountData, previousMonthData = null) {
    const reasons = [];

    // Check if account was archived during this specific month
    const monthStart = new Date(monthlyData.month + '-01');
    const monthEnd = new Date(monthStart.getFullYear(), monthStart.getMonth() + 1, 0);

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

      if (!hasCurrentMonthTexts) {
        reasons.push('Frozen & Inactive');
        return { level: 'high', reasons };
      }

      return { level: 'medium', reasons };
    }

    // Flags 4-8: LAUNCHED/ACTIVE accounts with proportional thresholds
    const daysInMonth = this.getDaysInMonth(monthlyData.month);
    const progressPercentage = (dayOfMonth - 1) / daysInMonth;

    // Avoid division by zero for first day of month
    if (progressPercentage <= 0) {
      reasons.push('No flags');
      return { level: 'low', reasons };
    }

    // Calculate proportional thresholds based on progress through month
    const proportionalRedemptionsThreshold = this.MONTHLY_REDEMPTIONS_THRESHOLD * progressPercentage;
    const proportionalLowEngagementRedemptionsThreshold = this.LOW_ENGAGEMENT_COMBO_REDEMPTIONS_THRESHOLD * progressPercentage;

    let flagCount = 0;
    const monthsSinceStart = this.calculateMonthsSinceStart(accountData.launched_at, monthlyData.month);

    // Flag 4: Monthly Redemptions (proportional) - 1 point
    if (monthlyData.total_coupons_redeemed < proportionalRedemptionsThreshold) {
      flagCount++;
      reasons.push('Low Monthly Redemptions');
    }

    // Flag 5: Low Engagement Combo - 2 points (only after 2 months)
    if (monthsSinceStart > 2) {
      if (monthlyData.avg_active_subs_cnt < this.LOW_ENGAGEMENT_COMBO_SUBS_THRESHOLD &&
          monthlyData.total_coupons_redeemed < proportionalLowEngagementRedemptionsThreshold) {
        flagCount += 2;
        reasons.push('Low Engagement Combo');
      }
    }

    // Flag 6: Low Activity - 1 point
    if (monthlyData.avg_active_subs_cnt < this.LOW_ACTIVITY_SUBS_THRESHOLD) {
      flagCount++;
      reasons.push('Low Activity');
    }

    // Flags 7 & 8: Drop flags using same-day comparisons (only after 3 months)
    if (previousMonthData && monthsSinceStart >= 3) {
      // Flag 7: Spend Drop - 1 point
      if (previousMonthData.total_spend > 0) {
        const spendDrop = Math.max(0, (previousMonthData.total_spend - monthlyData.total_spend) / previousMonthData.total_spend);
        if (spendDrop >= this.SPEND_DROP_THRESHOLD) {
          flagCount++;
          reasons.push('Spend Drop');
        }
      }

      // Flag 8: Redemptions Drop - 1 point
      if (previousMonthData.total_coupons_redeemed > 0) {
        const redemptionsDrop = Math.max(0, (previousMonthData.total_coupons_redeemed - monthlyData.total_coupons_redeemed) / previousMonthData.total_coupons_redeemed);
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

  // Calculate historical risk level for completed months (runs on 1st of following month)
  calculateHistoricalRiskLevel(monthData, accountData, previousMonthData = null) {
    const reasons = [];

    // Check if account was archived during this specific month
    const monthStart = new Date(monthData.month + '-01');
    const monthEnd = new Date(monthStart.getFullYear(), monthStart.getMonth() + 1, 0);

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

    // FROZEN accounts logic
    if (accountData.status === 'FROZEN') {
      const hasCurrentMonthTexts = monthData.total_texts_delivered > 0;
      reasons.push('Frozen Account Status');
      if (!hasCurrentMonthTexts) {
        reasons.push('Frozen & Inactive');
        return { level: 'high', reasons };
      }
      return { level: 'medium', reasons };
    }

    // LAUNCHED/ACTIVE accounts: Flag-based system
    let flagCount = 0;
    const monthsSinceStart = this.calculateMonthsSinceStart(accountData.launched_at, monthData.month);

    // Flag 2: Monthly Redemptions (< 10 redemptions) - 1 point
    if (monthData.total_coupons_redeemed < this.MONTHLY_REDEMPTIONS_THRESHOLD) {
      flagCount++;
      reasons.push('Low Monthly Redemptions');
    }

    // Flag 3: Low Engagement Combo (< 300 subs AND < 35 redemptions) - 2 points
    // Only available for accounts after their first two months
    if (monthsSinceStart > 2) {
      if (monthData.avg_active_subs_cnt < this.LOW_ENGAGEMENT_COMBO_SUBS_THRESHOLD &&
          monthData.total_coupons_redeemed < this.LOW_ENGAGEMENT_COMBO_REDEMPTIONS_THRESHOLD) {
        flagCount += 2;
        reasons.push('Low Engagement Combo');
      }
    }

    // Flag 4: Low Activity (< 300 subscribers per account) - 1 point
    if (monthData.avg_active_subs_cnt < this.LOW_ACTIVITY_SUBS_THRESHOLD) {
      flagCount++;
      reasons.push('Low Activity');
    }

    // Flag 5 & 6: Drop flags only available after month 3 with previous month data
    if (previousMonthData && monthsSinceStart >= 3) {
      // Flag 5: Spend Drop (≥ 40% decrease) - 1 point
      if (previousMonthData.total_spend > 0) {
        const spendDrop = Math.max(0, (previousMonthData.total_spend - monthData.total_spend) / previousMonthData.total_spend);
        if (spendDrop >= this.SPEND_DROP_THRESHOLD) {
          flagCount++;
          reasons.push('Spend Drop');
        }
      }

      // Flag 6: Redemptions Drop (≥ 50% decrease) - 1 point
      if (previousMonthData.total_coupons_redeemed > 0) {
        const redemptionsDrop = Math.max(0, (previousMonthData.total_coupons_redeemed - monthData.total_coupons_redeemed) / previousMonthData.total_coupons_redeemed);
        if (redemptionsDrop >= this.REDEMPTIONS_DROP_THRESHOLD) {
          flagCount++;
          reasons.push('Redemptions Drop');
        }
      }
    }

    // Determine risk level based on flag count
    let level = 'low';
    if (flagCount >= 3) level = 'high';
    else if (flagCount >= 1) level = 'medium';

    // Ensure low-risk accounts with no flags show "No flags" instead of empty array
    const finalReasons = reasons.length > 0 ? reasons : ['No flags'];

    return { level, reasons: finalReasons };
  }

  // Calculate historical risk levels for a completed month (runs on 1st of following month)
  async calculateHistoricalRiskForMonth(client, targetMonth) {
    console.log(`🎯 Calculating historical risk levels for completed month ${targetMonth}...`);

    // Get all monthly metrics for the target month with account details
    const accountsResult = await client.query(`
      SELECT
        mm.account_id, mm.month, mm.total_spend, mm.total_texts_delivered,
        mm.total_coupons_redeemed, mm.avg_active_subs_cnt,
        a.launched_at, a.status, a.archived_at, a.earliest_unit_archived_at
      FROM monthly_metrics mm
      JOIN accounts a ON mm.account_id = a.account_id
      WHERE mm.month = $1
      ORDER BY mm.account_id
    `, [targetMonth]);

    console.log(`📊 Processing ${accountsResult.rows.length} accounts for historical risk...`);

    // Get previous month for comparison
    const prevDate = new Date(targetMonth + '-01');
    prevDate.setMonth(prevDate.getMonth() - 1);
    const previousMonth = prevDate.toISOString().slice(0, 7);

    let accountsUpdated = 0;

    for (const account of accountsResult.rows) {
      // Get previous month data for comparison
      let previousMonthData = null;
      try {
        const prevResult = await client.query(`
          SELECT total_spend, total_coupons_redeemed, avg_active_subs_cnt, total_texts_delivered
          FROM monthly_metrics
          WHERE account_id = $1 AND month = $2
        `, [account.account_id, previousMonth]);

        if (prevResult.rows[0]) {
          previousMonthData = {
            total_spend: parseFloat(prevResult.rows[0].total_spend) || 0,
            total_coupons_redeemed: parseInt(prevResult.rows[0].total_coupons_redeemed) || 0,
            avg_active_subs_cnt: parseFloat(prevResult.rows[0].avg_active_subs_cnt) || 0,
            total_texts_delivered: parseInt(prevResult.rows[0].total_texts_delivered) || 0
          };
        }
      } catch (error) {
        // Previous month data not available - normal for new accounts
        previousMonthData = null;
      }

      // Calculate historical risk using the same algorithm as SQLite
      const riskResult = this.calculateHistoricalRiskLevel(account, account, previousMonthData);

      // Update historical_risk_level and risk_reasons, clear trending fields
      await client.query(`
        UPDATE monthly_metrics
        SET
          historical_risk_level = $1,
          risk_reasons = $2,
          trending_risk_level = NULL,
          trending_risk_reasons = NULL,
          updated_at = NOW()
        WHERE account_id = $3 AND month = $4
      `, [riskResult.level, JSON.stringify(riskResult.reasons), account.account_id, targetMonth]);

      accountsUpdated++;

      // Progress logging
      if (accountsUpdated % 100 === 0) {
        console.log(`   🎯 Updated ${accountsUpdated}/${accountsResult.rows.length} accounts...`);
      }
    }

    return accountsUpdated;
  }

  // Process historical rollup for a completed month
  async processHistoricalMonth(month) {
    await this.testConnection();

    const { targetMonth, monthLabel } = this.getMonthDetails(month);
    console.log(`📜 Processing historical rollup for ${monthLabel} (${targetMonth})...`);

    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      // Calculate historical risk levels for the completed month
      const accountsUpdated = await this.calculateHistoricalRiskForMonth(client, targetMonth);

      await client.query('COMMIT');

      console.log(`✅ Historical rollup completed for ${monthLabel}: ${accountsUpdated} accounts updated`);

      return {
        month: targetMonth,
        monthLabel: monthLabel,
        accountsProcessed: accountsUpdated
      };

    } catch (error) {
      await client.query('ROLLBACK');
      console.error(`❌ Historical rollup failed for ${targetMonth}:`, error);
      throw error;
    } finally {
      client.release();
    }
  }

  // Clean shutdown
  async close() {
    await this.pool.end();
    console.log('🔌 PostgreSQL connection pool closed');
  }
}

// CLI behavior - supports both trending (default) and historical rollups
if (import.meta.url === `file://${process.argv[1]}`) {
  const month = process.argv[2]; // Required: specify month as YYYY-MM
  const isHistorical = process.argv.includes('--historical');

  if (!month || !/^\d{4}-\d{2}$/.test(month)) {
    console.error('❌ Usage: node monthly-rollup-etl-postgres-native.js <YYYY-MM> [--historical]');
    console.error('   Example: node monthly-rollup-etl-postgres-native.js 2025-09');
    console.error('   Example: node monthly-rollup-etl-postgres-native.js 2025-08 --historical');
    console.error('   --historical flag: Calculate final historical risk levels for completed month');
    process.exit(1);
  }

  const etl = new MonthlyRollupETLPostgresNative();

  const processMethod = isHistorical ? etl.processHistoricalMonth(month) : etl.processMonth(month);
  const processType = isHistorical ? 'Historical monthly rollup' : 'Monthly rollup';

  processMethod
    .then(result => {
      console.log(`🎉 ${processType} ETL completed for ${result.monthLabel}!`);
      console.log(`📊 Summary: ${result.accountsProcessed} accounts processed for ${result.month}`);
      return etl.close();
    })
    .then(() => {
      process.exit(0);
    })
    .catch(error => {
      console.error(`❌ ${processType} ETL failed:`, error);
      etl.close().finally(() => process.exit(1));
    });
}

export { MonthlyRollupETLPostgresNative };