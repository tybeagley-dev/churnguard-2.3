import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import { createHubSpotService, formatRiskDataForHubSpot } from './hubspot.js';
import { hubspotIdTranslator } from './hubspot-id-translator.js';

export class HubSpotSyncService {
  constructor() {
    this.dbPath = process.env.SQLITE_DB_PATH || './data/churnguard_simulation.db';
  }

  async getDatabase() {
    const db = await open({
      filename: this.dbPath,
      driver: sqlite3.Database
    });
    return db;
  }

  async syncAccountsToHubSpot(targetDate = null, syncMode = 'daily') {
    const apiKey = process.env.HUBSPOT_API_KEY;
    if (!apiKey) {
      throw new Error('HUBSPOT_API_KEY environment variable is required');
    }

    const processDate = targetDate || this.getYesterday();
    const currentMonth = processDate.substring(0, 7);
    const previousMonth = this.getPreviousMonth(currentMonth);

    console.log(`🔄 Starting HubSpot sync for ${processDate} (${syncMode} mode)`);

    const hubspotService = createHubSpotService(apiKey);
    const db = await this.getDatabase();

    try {
      // Get month end date for eligibility criteria
      const monthEnd = this.getMonthEnd(currentMonth);

      const accounts = await db.all(`
        SELECT
          a.account_id,
          a.account_name,
          a.status,
          a.hubspot_id,
          a.archived_at,
          a.launched_at,
          a.earliest_unit_archived_at,
          mm_current.trending_risk_level,
          mm_current.trending_risk_reasons,
          mm_previous.historical_risk_level,
          mm_previous.risk_reasons,
          mm_current.total_spend,
          mm_current.total_texts_delivered,
          mm_current.total_coupons_redeemed,
          mm_current.avg_active_subs_cnt
        FROM accounts a
        LEFT JOIN monthly_metrics mm_current ON a.account_id = mm_current.account_id AND mm_current.month = ?
        LEFT JOIN monthly_metrics mm_previous ON a.account_id = mm_previous.account_id AND mm_previous.month = ?
        WHERE a.hubspot_id IS NOT NULL
          AND a.hubspot_id != ''
          AND a.hubspot_id != 'null'
          AND a.launched_at IS NOT NULL
          AND a.launched_at <= ? || ' 23:59:59'
          AND (
            COALESCE(a.archived_at, a.earliest_unit_archived_at) IS NULL
            OR COALESCE(a.archived_at, a.earliest_unit_archived_at) >= ? || '-01'
          )
      `, [currentMonth, previousMonth, monthEnd, currentMonth]);

      console.log(`📊 Found ${accounts.length} accounts with HubSpot IDs to sync`);

      const updates = [];
      let translatedCount = 0;

      for (const account of accounts) {
        let hubspotId = account.hubspot_id;

        const hasTranslation = await hubspotIdTranslator.hasTranslation(account.account_id);
        if (hasTranslation) {
          hubspotId = await hubspotIdTranslator.getCorrectHubSpotId(account.account_id, account.hubspot_id);
          translatedCount++;
        }

        const riskData = this.formatRiskDataForV2_3(account);

        updates.push({
          hubspotId,
          accountName: account.account_name,
          riskData
        });
      }

      console.log(`🔄 Syncing ${updates.length} accounts to HubSpot (${translatedCount} with ID translations)`);

      const syncResult = await hubspotService.bulkUpdateCompanyRiskData(updates, syncMode);

      console.log(`✅ HubSpot sync completed:`);
      console.log(`   - Total accounts: ${syncResult.totalAccounts}`);
      console.log(`   - Successful syncs: ${syncResult.successfulSyncs}`);
      console.log(`   - Failed syncs: ${syncResult.failedSyncs}`);
      console.log(`   - Risk breakdown: High: ${syncResult.summary.highRiskSynced}, Medium: ${syncResult.summary.mediumRiskSynced}, Low: ${syncResult.summary.lowRiskSynced}`);

      if (syncResult.failedSyncs > 0) {
        console.log(`⚠️  Failed sync details:`);
        syncResult.results
          .filter(r => !r.success)
          .slice(0, 5)
          .forEach(r => console.log(`   - ${r.accountName} (${r.hubspotId}): ${r.error}`));

        if (syncResult.failedSyncs > 5) {
          console.log(`   - ... and ${syncResult.failedSyncs - 5} more failures`);
        }
      }

      // Note: Ineligible account sync removed - going forward only eligible accounts will be synced

      await db.close();

      return {
        success: true,
        processDate,
        syncMode,
        totalAccounts: syncResult.totalAccounts,
        successfulSyncs: syncResult.successfulSyncs,
        failedSyncs: syncResult.failedSyncs,
        translatedCount,
        summary: syncResult.summary
      };

    } catch (error) {
      await db.close();
      console.error(`❌ HubSpot sync failed:`, error);
      throw error;
    }
  }

  formatRiskDataForV2_3(account) {
    const isFrozenAccount = account.status === 'FROZEN';
    const isArchivedAccount = account.status === 'ARCHIVED' || account.archived_at;

    let currentRiskLevel;
    let currentRiskReasons;
    let trendingRiskLevel;
    let trendingRiskReasons;

    if (isArchivedAccount) {
      // For archived accounts, use previous month's historical risk as current, trending always High
      currentRiskLevel = account.historical_risk_level || 'Low';

      // Handle risk reasons for archived accounts
      if (account.risk_reasons) {
        try {
          const parsedReasons = JSON.parse(account.risk_reasons);
          currentRiskReasons = Array.isArray(parsedReasons) ? parsedReasons.join(', ') : String(parsedReasons);
        } catch {
          currentRiskReasons = account.risk_reasons;
        }
      } else {
        // For accounts archived in their first month with no historical data
        currentRiskReasons = 'Insufficient Data';
      }

      trendingRiskLevel = 'High';
      trendingRiskReasons = 'Archived';
    } else if (isFrozenAccount) {
      // For frozen accounts, use previous month's historical risk as current
      currentRiskLevel = account.historical_risk_level;
      currentRiskReasons = account.risk_reasons || 'Frozen Account';
      trendingRiskLevel = account.trending_risk_level || 'Medium';
      trendingRiskReasons = account.trending_risk_reasons || 'Frozen Account';
    } else {
      // For active accounts, current risk comes from previous month's historical risk
      // If no previous month data (new accounts), leave current risk empty
      currentRiskLevel = account.historical_risk_level || null;
      currentRiskReasons = account.risk_reasons || (currentRiskLevel ? 'No active risk factors' : null);
      trendingRiskLevel = account.trending_risk_level || 'Low';

      try {
        const parsedReasons = JSON.parse(account.trending_risk_reasons || '["No flags"]');
        trendingRiskReasons = Array.isArray(parsedReasons) ? parsedReasons.join(', ') : String(parsedReasons);
      } catch {
        trendingRiskReasons = account.trending_risk_reasons || 'No active risk factors';
      }
    }

    // Capitalize risk levels if they exist
    if (currentRiskLevel) {
      currentRiskLevel = currentRiskLevel.charAt(0).toUpperCase() + currentRiskLevel.slice(1);
    }
    if (trendingRiskLevel) {
      trendingRiskLevel = trendingRiskLevel.charAt(0).toUpperCase() + trendingRiskLevel.slice(1);
    }

    const lastUpdated = new Date().toISOString().split('T')[0];

    return {
      churnguard_current_risk_level: currentRiskLevel,
      churnguard_current_risk_reasons: currentRiskReasons,
      churnguard_trending_risk_level: trendingRiskLevel,
      churnguard_trending_risk_reasons: trendingRiskReasons,
      churnguard_last_updated: lastUpdated
    };
  }

  async syncIneligibleAccountsWithYesterdayDate(currentMonth, hubspotService) {
    const db = await this.getDatabase();
    const monthEnd = this.getMonthEnd(currentMonth);
    const yesterdayDate = this.getYesterday();

    try {
      // Get accounts with HubSpot IDs that DON'T meet eligibility criteria
      const ineligibleAccounts = await db.all(`
        SELECT
          a.account_id,
          a.account_name,
          a.status,
          a.hubspot_id,
          a.archived_at,
          a.launched_at,
          a.earliest_unit_archived_at
        FROM accounts a
        WHERE a.hubspot_id IS NOT NULL
          AND a.hubspot_id != ''
          AND a.hubspot_id != 'null'
          AND NOT (
            a.launched_at IS NOT NULL
            AND a.launched_at <= ? || ' 23:59:59'
            AND (
              COALESCE(a.archived_at, a.earliest_unit_archived_at) IS NULL
              OR COALESCE(a.archived_at, a.earliest_unit_archived_at) >= ? || '-01'
            )
          )
      `, [monthEnd, currentMonth]);

      console.log(`📊 Found ${ineligibleAccounts.length} ineligible accounts to mark with yesterday's date`);

      if (ineligibleAccounts.length === 0) {
        await db.close();
        return { totalIneligible: 0, successfulSyncs: 0, failedSyncs: 0 };
      }

      const updates = [];
      let translatedCount = 0;

      for (const account of ineligibleAccounts) {
        let hubspotId = account.hubspot_id;

        const hasTranslation = await hubspotIdTranslator.hasTranslation(account.account_id);
        if (hasTranslation) {
          hubspotId = await hubspotIdTranslator.getCorrectHubSpotId(account.account_id, account.hubspot_id);
          translatedCount++;
        }

        // Create full risk data with yesterday's date for filtering
        const riskData = {
          churnguard_current_risk_level: null,
          churnguard_current_risk_reasons: null,
          churnguard_trending_risk_level: null,
          churnguard_trending_risk_reasons: null,
          churnguard_last_updated: yesterdayDate
        };

        updates.push({
          hubspotId,
          accountName: account.account_name,
          riskData
        });
      }

      console.log(`🔄 Syncing ${updates.length} ineligible accounts with yesterday's date (${translatedCount} with ID translations)`);

      // Use the proven bulkUpdateCompanyRiskData method for ineligible accounts
      const syncResult = await hubspotService.bulkUpdateCompanyRiskData(updates, 'ineligible-cleanup');

      console.log(`✅ Ineligible accounts sync completed:`);
      console.log(`   - Total ineligible accounts: ${syncResult.totalAccounts}`);
      console.log(`   - Successful syncs: ${syncResult.successfulSyncs}`);
      console.log(`   - Failed syncs: ${syncResult.failedSyncs}`);

      await db.close();

      return {
        totalIneligible: syncResult.totalAccounts,
        successfulSyncs: syncResult.successfulSyncs,
        failedSyncs: syncResult.failedSyncs,
        translatedCount
      };

    } catch (error) {
      await db.close();
      console.error(`❌ Ineligible accounts sync failed:`, error);
      throw error;
    }
  }

  async testHubSpotConnection() {
    const apiKey = process.env.HUBSPOT_API_KEY;
    if (!apiKey) {
      return {
        success: false,
        message: 'HUBSPOT_API_KEY environment variable is not set'
      };
    }

    const hubspotService = createHubSpotService(apiKey);
    return await hubspotService.testConnection();
  }

  getYesterday() {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    return yesterday.toISOString().split('T')[0];
  }

  getMonthEnd(yearMonth) {
    const [year, month] = yearMonth.split('-');
    const lastDay = new Date(parseInt(year), parseInt(month), 0).getDate();
    return `${yearMonth}-${lastDay.toString().padStart(2, '0')}`;
  }

  getPreviousMonth(yearMonth) {
    const [year, month] = yearMonth.split('-');
    const date = new Date(parseInt(year), parseInt(month) - 1, 1);
    date.setMonth(date.getMonth() - 1);
    return `${date.getFullYear()}-${(date.getMonth() + 1).toString().padStart(2, '0')}`;
  }
}