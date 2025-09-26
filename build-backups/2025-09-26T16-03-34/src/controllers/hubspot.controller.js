import { HubSpotSyncService } from '../services/hubspot-sync.js';
import { createHubSpotService } from '../services/hubspot.js';

const hubspotSync = new HubSpotSyncService();

export const testHubSpotConnection = async (req, res) => {
  try {
    const result = await hubspotSync.testHubSpotConnection();
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `HubSpot connection test failed: ${error.message}`
    });
  }
};

export const syncAllAccounts = async (req, res) => {
  try {
    const { targetDate, syncMode = 'manual' } = req.body;

    const result = await hubspotSync.syncAccountsToHubSpot(targetDate, syncMode);

    res.json({
      success: true,
      message: `HubSpot sync completed successfully`,
      data: result
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `HubSpot sync failed: ${error.message}`,
      error: error.message
    });
  }
};

export const getHubSpotSampleData = async (req, res) => {
  try {
    const apiKey = process.env.HUBSPOT_API_KEY;
    if (!apiKey) {
      return res.status(400).json({
        success: false,
        message: 'HUBSPOT_API_KEY environment variable is not configured'
      });
    }

    const hubspotService = createHubSpotService(apiKey);
    const result = await hubspotService.getSampleCompanies(5);

    res.json({
      success: true,
      data: result
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `Failed to fetch HubSpot sample data: ${error.message}`
    });
  }
};

export const getHubSpotStatus = async (req, res) => {
  try {
    const apiKeyConfigured = !!process.env.HUBSPOT_API_KEY;

    let connectionStatus = null;
    if (apiKeyConfigured) {
      connectionStatus = await hubspotSync.testHubSpotConnection();
    }

    res.json({
      success: true,
      status: {
        apiKeyConfigured,
        connectionStatus
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `Failed to get HubSpot status: ${error.message}`
    });
  }
};