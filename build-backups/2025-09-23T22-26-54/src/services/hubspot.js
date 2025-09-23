class HubSpotService {
  constructor(apiKey) {
    this.apiKey = apiKey;
    this.baseUrl = 'https://api.hubapi.com';
  }

  async updateCompanyRiskData(hubspotId, riskData, accountName = 'Unknown', syncMode = 'daily') {
    try {
      const properties = {
        churnguard_current_risk_level: riskData.churnguard_current_risk_level,
        churnguard_current_risk_reasons: riskData.churnguard_current_risk_reasons,
        churnguard_trending_risk_level: riskData.churnguard_trending_risk_level,
        churnguard_trending_risk_reasons: riskData.churnguard_trending_risk_reasons,
        churnguard_last_updated: riskData.churnguard_last_updated
      };

      const response = await fetch(`${this.baseUrl}/crm/v3/objects/companies/${hubspotId}`, {
        method: 'PATCH',
        headers: {
          'Authorization': `Bearer ${this.apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ properties })
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HubSpot API error: ${response.status} - ${errorText}`);
      }

      const result = await response.json();

      return {
        success: true,
        hubspotId,
        accountName,
        updatedProperties: Object.keys(properties)
      };
    } catch (error) {
      return {
        success: false,
        hubspotId,
        accountName,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  async bulkUpdateCompanyRiskData(updates, syncMode = 'daily') {
    const results = [];
    const summary = {
      highRiskSynced: 0,
      mediumRiskSynced: 0,
      lowRiskSynced: 0
    };

    const batchSize = 10;
    for (let i = 0; i < updates.length; i += batchSize) {
      const batch = updates.slice(i, i + batchSize);

      const batchPromises = batch.map(async (update, index) => {
        if (index > 0) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }

        const result = await this.updateCompanyRiskData(
          update.hubspotId,
          update.riskData,
          update.accountName,
          syncMode
        );

        if (result.success) {
          const riskLevel = update.riskData.churnguard_current_risk_level?.toLowerCase();
          if (riskLevel === 'high') summary.highRiskSynced++;
          else if (riskLevel === 'medium') summary.mediumRiskSynced++;
          else if (riskLevel === 'low') summary.lowRiskSynced++;
        }

        return result;
      });

      const batchResults = await Promise.all(batchPromises);
      results.push(...batchResults);

      if (i + batchSize < updates.length) {
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }

    const successfulSyncs = results.filter(r => r.success).length;
    const failedSyncs = results.filter(r => !r.success).length;

    return {
      totalAccounts: updates.length,
      successfulSyncs,
      failedSyncs,
      results,
      summary
    };
  }

  async testConnection() {
    try {
      const response = await fetch(`${this.baseUrl}/crm/v3/objects/companies?limit=1`, {
        headers: {
          'Authorization': `Bearer ${this.apiKey}`,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        const errorText = await response.text();
        return {
          success: false,
          message: `HubSpot API connection failed: ${response.status} - ${errorText}`
        };
      }

      const data = await response.json();

      return {
        success: true,
        message: `HubSpot API connected successfully. Found ${data.total || 0} companies.`,
        permissions: ['read', 'write']
      };
    } catch (error) {
      return {
        success: false,
        message: `HubSpot API connection error: ${error instanceof Error ? error.message : String(error)}`
      };
    }
  }

  async getSampleCompanies(limit = 5) {
    try {
      const response = await fetch(
        `${this.baseUrl}/crm/v3/objects/companies?limit=${limit}&properties=name,domain,hs_object_id`,
        {
          headers: {
            'Authorization': `Bearer ${this.apiKey}`,
            'Content-Type': 'application/json'
          }
        }
      );

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to fetch companies: ${response.status} - ${errorText}`);
      }

      const data = await response.json();

      return {
        success: true,
        companies: data.results || []
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  async updateLastUpdatedDate(hubspotId, date, accountName = 'Unknown') {
    try {
      const url = `${this.baseUrl}/crm/v3/objects/companies/${hubspotId}`;
      const response = await fetch(url, {
        method: 'PATCH',
        headers: {
          'Authorization': `Bearer ${this.apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          properties: {
            churnguard_last_updated: date
          }
        })
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(`${response.status} - ${JSON.stringify(errorData)}`);
      }

      return {
        success: true,
        hubspotId,
        accountName
      };
    } catch (error) {
      return {
        success: false,
        hubspotId,
        accountName,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }
}

export function createHubSpotService(apiKey) {
  return new HubSpotService(apiKey);
}

export function formatRiskDataForHubSpot(account) {
  const isFrozenAccount = account.status === 'FROZEN' || account.account_status === 'FROZEN';
  const isArchivedAccount = account.status === 'ARCHIVED' || account.account_status === 'ARCHIVED' || account.archived_flag === 1;

  let currentRiskLevel;
  let currentRiskReasons;
  let trendingRiskLevel;
  let trendingRiskReasons;

  if (isArchivedAccount) {
    const flagCount = [
      account.low_engagement_combo_flag,
      account.monthly_redemptions_flag,
      account.no_spend_flag
    ].filter(flag => flag === 1).length;

    if (flagCount >= 2) currentRiskLevel = 'High';
    else if (flagCount === 1) currentRiskLevel = 'Medium';
    else currentRiskLevel = 'Low';

    const activeFlags = [];
    if (account.low_engagement_combo_flag === 1) activeFlags.push('Low Engagement');
    if (account.monthly_redemptions_flag === 1) activeFlags.push('Low Monthly Redemptions');
    if (account.no_spend_flag === 1) activeFlags.push('No Spend');

    currentRiskReasons = activeFlags.length > 0
      ? activeFlags.join(', ')
      : 'No active risk factors';

    trendingRiskLevel = 'High';
    trendingRiskReasons = 'Archived';

  } else if (isFrozenAccount) {
    currentRiskLevel = 'Medium';
    currentRiskReasons = account.risk_reason || account.current_risk_reasons || 'Frozen';
    trendingRiskLevel = account.trending_risk_level || 'Medium';
    trendingRiskReasons = account.trending_risk_reason || account.trending_risk_reasons || currentRiskReasons;

  } else {
    currentRiskLevel = account.risk_level || 'Low';
    currentRiskLevel = currentRiskLevel.charAt(0).toUpperCase() + currentRiskLevel.slice(1);

    const activeFlags = [];
    if (account.low_engagement_combo_flag === 1) activeFlags.push('Low Engagement');
    if (account.frozen_no_texts_flag === 1) activeFlags.push('No Texts Sent');
    if (account.frozen_with_texts_flag === 1) activeFlags.push('Frozen with Texts');
    if (account.monthly_redemptions_flag === 1) activeFlags.push('Low Monthly Redemptions');
    if (account.no_spend_flag === 1) activeFlags.push('No Spend');

    currentRiskReasons = activeFlags.length > 0
      ? activeFlags.join(', ')
      : 'No active risk factors';

    trendingRiskLevel = account.trending_risk_level || currentRiskLevel;
    trendingRiskLevel = trendingRiskLevel.charAt(0).toUpperCase() + trendingRiskLevel.slice(1);
    trendingRiskReasons = currentRiskReasons;
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