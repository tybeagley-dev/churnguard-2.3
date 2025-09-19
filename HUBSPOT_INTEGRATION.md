# HubSpot Integration for ChurnGuard v2.3

## Overview

ChurnGuard v2.3 includes a comprehensive HubSpot integration that automatically syncs risk assessment data to HubSpot CRM companies. This integration is built into the daily ETL pipeline and runs automatically when configured.

## Architecture

### Core Components

1. **HubSpot API Service** (`src/services/hubspot.js`)
   - Main service for interacting with HubSpot CRM API
   - Handles rate limiting and batch processing
   - Updates 5 custom company properties with risk data

2. **HubSpot ID Translator** (`src/services/hubspot-id-translator.js`)
   - Corrects HubSpot company ID mappings for accounts with incorrect IDs
   - Uses pre-built translation table with 36 corrected mappings

3. **HubSpot Sync Service** (`src/services/hubspot-sync.js`)
   - Orchestrates the sync process between ChurnGuard and HubSpot
   - Handles data formatting and error handling
   - Integrates with daily ETL pipeline

4. **Translation Table** (`assets/churnguard_translation_table_corrected.json`)
   - Contains 36 corrected HubSpot company ID mappings
   - Includes high-confidence corrections for major accounts like Pizza Planet, Pizza Factory, etc.

## Configuration

### Environment Variables

Add to your `.env` file:

```bash
# HubSpot Integration (Optional)
# Get your API key from: HubSpot > Settings > Integrations > API key
HUBSPOT_API_KEY=your-hubspot-api-key-here
```

### HubSpot Custom Properties

The integration updates these 5 custom company properties in HubSpot:

1. `churnguard_current_risk_level` - Current risk level (High/Medium/Low)
2. `churnguard_current_risk_reasons` - Current risk factors
3. `churnguard_trending_risk_level` - Trending risk level (High/Medium/Low)
4. `churnguard_trending_risk_reasons` - Trending risk factors
5. `churnguard_last_updated` - Last sync date (YYYY-MM-DD)

## Integration Points

### Daily ETL Pipeline

The HubSpot sync is automatically included as **Step 6** in the daily ETL pipeline:

```
Step 1: Update accounts table from BigQuery
Step 2: Extract from BigQuery and Load to daily_metrics
Step 3: Aggregate to monthly_metrics
Step 4: Update trending risk levels
Step 5: Update account summary metrics
Step 6: Sync risk data to HubSpot ✨ NEW
```

### API Endpoints

The integration provides these REST endpoints:

- `GET /api/hubspot/status` - Check configuration and connection status
- `GET /api/hubspot/test-connection` - Test HubSpot API connectivity
- `GET /api/hubspot/sample-data` - Get sample HubSpot company data
- `POST /api/hubspot/sync` - Manually trigger sync process

## Risk Data Mapping

### Account Status Handling

1. **ARCHIVED Accounts**
   - Current: Shows historical risk data from when account was active
   - Trending: Always "High" with "Archived" reason

2. **FROZEN Accounts**
   - Current: Shows "Medium" risk with "Frozen" reason
   - Trending: Uses calculated trending risk data

3. **ACTIVE/LAUNCHED Accounts**
   - Current: Uses historical risk level (from completed months)
   - Trending: Uses real-time trending risk assessment

### Risk Level Calculation

Risk levels are determined by the v2.3 8-flag system:
- **High Risk**: 3+ flags OR Account Frozen and 1+ months since last text OR Account Archived
- **Medium Risk**: 1-2 flags OR Account Frozen status
- **Low Risk**: 0 flags

## Sync Process

### Daily Automatic Sync

1. **Triggered**: After daily ETL completes risk calculations
2. **Scope**: All accounts with valid HubSpot IDs in current month
3. **ID Translation**: Automatically applies corrections for 36 known accounts
4. **Batch Processing**: Processes in batches of 10 with rate limiting
5. **Error Handling**: Failed syncs don't stop ETL pipeline

### Manual Sync

```bash
# Via API
curl -X POST http://localhost:3004/api/hubspot/sync \
  -H "Content-Type: application/json" \
  -d '{"targetDate": "2025-09-18", "syncMode": "manual"}'

# Via ETL Script
node -e "
import { HubSpotSyncService } from './src/services/hubspot-sync.js';
const sync = new HubSpotSyncService();
await sync.syncAccountsToHubSpot('2025-09-18', 'manual');
"
```

## Features

### Rate Limiting
- Batches of 10 companies per batch
- 100ms delay between requests within batch
- 500ms delay between batches
- Prevents HubSpot API rate limit violations

### ID Translation
- Automatically corrects 36 known incorrect HubSpot IDs
- High-confidence mappings for major accounts
- Transparent logging of ID corrections

### Error Resilience
- Failed HubSpot syncs don't stop daily ETL
- Detailed error logging for troubleshooting
- Graceful degradation when API key not configured

### Comprehensive Logging
```
🔄 Starting HubSpot sync for 2025-09-18 (daily mode)
📊 Found 847 accounts with HubSpot IDs to sync
🔄 Syncing 847 accounts to HubSpot (36 with ID translations)
✅ HubSpot sync completed:
   - Total accounts: 847
   - Successful syncs: 845
   - Failed syncs: 2
   - Risk breakdown: High: 45, Medium: 234, Low: 566
```

## Testing

### Connection Test
```bash
curl http://localhost:3004/api/hubspot/status
```

### Manual Sync Test
```bash
curl -X POST http://localhost:3004/api/hubspot/sync \
  -H "Content-Type: application/json" \
  -d '{"syncMode": "test"}'
```

## Deployment

### Production Setup

1. **Add HubSpot API Key to Render Environment Variables**
   ```
   HUBSPOT_API_KEY=pat-na1-xxxxx-your-actual-key
   ```

2. **Verify Custom Properties in HubSpot**
   - Ensure all 5 custom properties exist in your HubSpot account
   - Properties should be of type "Single-line text" except date field

3. **Monitor Initial Sync**
   - Check logs for successful bulk sync on first deployment
   - Verify data appears correctly in HubSpot companies

### Monitoring

- HubSpot sync results are included in daily ETL logs
- Failed syncs are logged but don't stop the pipeline
- Connection status available via `/api/hubspot/status`

## Files Added/Modified

### New Files
- `src/services/hubspot.js` - HubSpot API service
- `src/services/hubspot-id-translator.js` - ID translation service
- `src/services/hubspot-sync.js` - Sync orchestration service
- `src/controllers/hubspot.controller.js` - API controllers
- `src/routes/hubspot.routes.js` - API routes
- `assets/churnguard_translation_table_corrected.json` - ID translation data

### Modified Files
- `etl/daily-production-etl.js` - Added HubSpot sync step
- `server-clean.js` - Added HubSpot routes
- `.env.example` - Added HubSpot configuration

## Benefits

1. **Automated Risk Sync**: Sales team sees real-time risk data in HubSpot
2. **Historical Context**: Both current and trending risk assessments
3. **Clean Data**: ID translation ensures accurate company matching
4. **Reliable**: Built-in error handling and rate limiting
5. **Transparent**: Comprehensive logging and monitoring
6. **Optional**: Gracefully disabled when API key not configured

The HubSpot integration provides a seamless bridge between ChurnGuard's advanced risk analytics and HubSpot's CRM capabilities, enabling data-driven customer success workflows.