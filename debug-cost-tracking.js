#!/usr/bin/env node

/**
 * Debug script to test BigQuery cost tracking without database dependencies
 * Usage: node debug-cost-tracking.js
 */

import { BigQuery } from '@google-cloud/bigquery';
import dotenv from 'dotenv';

// Load environment from .env file for local testing
dotenv.config();

class CostTrackingDebugger {
  constructor() {
    console.log('🔧 [DEBUG] Starting cost tracking debugger...');

    // Debug environment variables
    console.log(`🔧 [DEBUG] BIGQUERY_COST_TRACKING: "${process.env.BIGQUERY_COST_TRACKING}"`);
    console.log(`🔧 [DEBUG] BIGQUERY_COST_TRACKING_LEVEL: "${process.env.BIGQUERY_COST_TRACKING_LEVEL}"`);
    console.log(`🔧 [DEBUG] GOOGLE_CLOUD_PROJECT_ID: "${process.env.GOOGLE_CLOUD_PROJECT_ID}"`);
    console.log(`🔧 [DEBUG] Has GOOGLE_APPLICATION_CREDENTIALS_JSON: ${!!process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON}`);

    // Cost tracking configuration
    this.enableCostTracking = process.env.BIGQUERY_COST_TRACKING === 'true';
    this.costTrackingLevel = process.env.BIGQUERY_COST_TRACKING_LEVEL || 'summary';

    console.log(`🔧 [DEBUG] Cost tracking enabled: ${this.enableCostTracking}`);
    console.log(`🔧 [DEBUG] Cost tracking level: ${this.costTrackingLevel}`);

    if (!this.enableCostTracking) {
      console.log('❌ Cost tracking is DISABLED. Set BIGQUERY_COST_TRACKING=true to enable.');
      return;
    }

    // Initialize BigQuery
    const bigqueryConfig = {
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
    };

    if (process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) {
      try {
        bigqueryConfig.credentials = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON);
        console.log('✅ Using JSON credentials');
      } catch (error) {
        console.error('❌ Failed to parse GOOGLE_APPLICATION_CREDENTIALS_JSON:', error);
        return;
      }
    } else if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      bigqueryConfig.keyFilename = process.env.GOOGLE_APPLICATION_CREDENTIALS;
      console.log('✅ Using keyfile credentials');
    } else {
      console.error('❌ No BigQuery credentials found');
      return;
    }

    this.bigquery = new BigQuery(bigqueryConfig);
    console.log('✅ BigQuery client initialized');
  }

  // Calculate cost using real billing data
  calculateActualQueryCost(statistics) {
    const costPerTB = 6.25;
    const bytesBilled = parseInt(statistics.totalBytesBilled || statistics.totalBytesProcessed || 0);
    const tbBilled = bytesBilled / (1024 ** 4);

    return {
      actualCostUSD: (tbBilled * costPerTB).toFixed(6),
      bytesBilled: bytesBilled,
      bytesProcessed: parseInt(statistics.totalBytesProcessed || 0),
      pricingModel: statistics.totalBytesBilled ? 'actual-billed' : 'estimated-processed'
    };
  }

  // Get dry-run cost estimate
  async getDryRunCostEstimate(query) {
    if (!this.enableCostTracking) {
      return null;
    }

    try {
      console.log('🧪 Running dry-run cost estimate...');
      const [job] = await this.bigquery.createQueryJob({
        query,
        location: 'US',
        dryRun: true
      });

      const metadata = job.metadata;
      const costData = this.calculateActualQueryCost(metadata.statistics || {});

      console.log(`💰 [PRE-ESTIMATE] Estimated: $${costData.actualCostUSD} (${costData.bytesProcessed} bytes)`);

      return {
        estimatedCostUSD: costData.actualCostUSD,
        estimatedBytesProcessed: costData.bytesProcessed,
        isDryRun: true
      };
    } catch (error) {
      console.warn(`⚠️  Dry-run cost estimation failed: ${error.message}`);
      return null;
    }
  }

  // Test with a simple query
  async testCostTracking() {
    if (!this.enableCostTracking) {
      console.log('❌ Cost tracking disabled - exiting test');
      return;
    }

    console.log('🚀 Testing cost tracking with simple query...');

    // Simple test query that should process minimal data
    const testQuery = `
      SELECT COUNT(*) as total_accounts
      FROM accounts.accounts
      WHERE status IN ('ACTIVE', 'LAUNCHED')
      LIMIT 1
    `;

    try {
      // Step 1: Get dry-run estimate
      const dryRunEstimate = await this.getDryRunCostEstimate(testQuery);

      // Step 2: Execute actual query
      console.log('🚀 Executing actual query...');
      const startTime = Date.now();

      const [job] = await this.bigquery.createQueryJob({
        query: testQuery,
        location: 'US',
        dryRun: false
      });

      const [rows] = await job.getQueryResults();
      const metadata = job.metadata;
      const duration = Date.now() - startTime;

      // Step 3: Calculate actual cost
      const costData = this.calculateActualQueryCost(metadata.statistics || {});

      // Step 4: Log results
      const costInfo = {
        jobId: metadata.id,
        queryPreview: testQuery.substring(0, 100).replace(/\s+/g, ' ') + '...',
        totalSlotMs: metadata.statistics?.totalSlotMs || '0',
        duration: duration,
        dryRunEstimate: dryRunEstimate,
        ...costData
      };

      this.logCostMetrics(costInfo);

      console.log(`✅ Test completed - found ${rows[0]?.total_accounts} accounts`);

    } catch (error) {
      console.error(`❌ Test failed: ${error.message}`);
    }
  }

  // Log cost metrics
  logCostMetrics(costInfo) {
    const timestamp = new Date().toISOString();

    if (this.costTrackingLevel === 'summary') {
      const pricingType = costInfo.pricingModel === 'actual-billed' ? 'ACTUAL' : 'EST';
      console.log(`💰 [COST-TRACKING] ${timestamp} | Job: ${costInfo.jobId} | Billed: ${costInfo.bytesBilled} bytes | ${pricingType} Cost: $${costInfo.actualCostUSD} | Duration: ${costInfo.duration}ms`);

      if (costInfo.dryRunEstimate) {
        console.log(`💰 [PRE-ESTIMATE] ${timestamp} | Estimated: $${costInfo.dryRunEstimate.estimatedCostUSD} (${costInfo.dryRunEstimate.estimatedBytesProcessed} bytes)`);
      }
    } else if (this.costTrackingLevel === 'detailed') {
      console.log(`💰 [COST-TRACKING-DETAILED] ${timestamp}`);
      console.log(`   📊 Job ID: ${costInfo.jobId}`);
      console.log(`   💰 Pricing Model: ${costInfo.pricingModel}`);
      console.log(`   📏 Bytes Billed: ${costInfo.bytesBilled} (${(costInfo.bytesBilled / (1024**3)).toFixed(2)} GB)`);
      console.log(`   📏 Bytes Processed: ${costInfo.bytesProcessed} (${(costInfo.bytesProcessed / (1024**3)).toFixed(2)} GB)`);
      console.log(`   ⚡ Slot Milliseconds: ${costInfo.totalSlotMs}`);
      console.log(`   ⏱️  Query Duration: ${costInfo.duration}ms`);
      console.log(`   💵 Actual Cost: $${costInfo.actualCostUSD}`);
      console.log(`   🔍 Query Preview: ${costInfo.queryPreview}`);

      if (costInfo.dryRunEstimate) {
        console.log(`   🧪 Pre-Run Estimate: $${costInfo.dryRunEstimate.estimatedCostUSD}`);
        const accuracy = ((parseFloat(costInfo.dryRunEstimate.estimatedCostUSD) / parseFloat(costInfo.actualCostUSD)) * 100).toFixed(1);
        console.log(`   🎯 Estimation Accuracy: ${accuracy}%`);
      }
    }
  }
}

// Run the test
const debugger = new CostTrackingDebugger();
if (debugger.enableCostTracking && debugger.bigquery) {
  debugger.testCostTracking()
    .then(() => {
      console.log('🎉 Cost tracking debug test completed!');
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Debug test failed:', error);
      process.exit(1);
    });
} else {
  console.log('❌ Cost tracking test skipped - requirements not met');
  process.exit(1);
}