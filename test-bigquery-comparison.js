import { BigQuery } from '@google-cloud/bigquery';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

class BigQueryETLComparison {
  constructor() {
    this.bigquery = new BigQuery({
      projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
    });
    this.dbPath = process.env.SQLITE_DB_PATH || './data/churnguard_simulation.db';
  }

  async getDatabase() {
    const db = await open({
      filename: this.dbPath,
      driver: sqlite3.Database
    });
    return db;
  }

  async getSpendDataFromBigQuery(date) {
    console.log(`💰 Fetching spend data from BigQuery for ${date}...`);

    const query = `
      WITH account_unit_archive_dates AS (
        SELECT
          u.account_id,
          MIN(u.archived_at) as earliest_unit_archived_at
        FROM units.units u
        WHERE u.status = 'ARCHIVED' AND u.archived_at IS NOT NULL
        GROUP BY u.account_id
      )

      SELECT
        tr.account_id,
        SUM(tr.total) as total_spend
      FROM dbt_models.total_revenue_by_account_and_date tr
      INNER JOIN accounts.accounts a ON tr.account_id = a.id
      LEFT JOIN account_unit_archive_dates aad ON a.id = aad.account_id
      WHERE tr.date = DATE('${date}')
        AND a.launched_at IS NOT NULL
        AND DATE(a.launched_at) <= DATE('${date}')
        AND (
          COALESCE(a.archived_at, aad.earliest_unit_archived_at) IS NULL
          OR DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)) > LAST_DAY(DATE('${date}'))
        )
      GROUP BY tr.account_id
      HAVING total_spend > 0
      ORDER BY tr.account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found spend data for ${rows.length} eligible accounts from BigQuery`);
    return rows;
  }

  async getTextsDataFromBigQuery(date) {
    console.log(`📱 Fetching texts data from BigQuery for ${date}...`);

    const query = `
      WITH account_unit_archive_dates AS (
        SELECT
          u.account_id,
          MIN(u.archived_at) as earliest_unit_archived_at
        FROM units.units u
        WHERE u.status = 'ARCHIVED' AND u.archived_at IS NOT NULL
        GROUP BY u.account_id
      )

      SELECT
        u.account_id,
        COUNT(DISTINCT t.id) as total_texts_delivered
      FROM public.texts t
      JOIN units.units u ON u.id = t.unit_id
      INNER JOIN accounts.accounts a ON u.account_id = a.id
      LEFT JOIN account_unit_archive_dates aad ON a.id = aad.account_id
      WHERE t.direction = 'OUTGOING'
        AND t.status = 'DELIVERED'
        AND DATE(t.created_at) = DATE('${date}')
        AND a.launched_at IS NOT NULL
        AND DATE(a.launched_at) <= DATE('${date}')
        AND (
          COALESCE(a.archived_at, aad.earliest_unit_archived_at) IS NULL
          OR DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)) > LAST_DAY(DATE('${date}'))
        )
      GROUP BY u.account_id
      HAVING total_texts_delivered > 0
      ORDER BY u.account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found texts data for ${rows.length} eligible accounts from BigQuery`);
    return rows;
  }

  async getCouponsDataFromBigQuery(date) {
    console.log(`🎫 Fetching coupons data from BigQuery for ${date}...`);

    const query = `
      WITH account_unit_archive_dates AS (
        SELECT
          u.account_id,
          MIN(u.archived_at) as earliest_unit_archived_at
        FROM units.units u
        WHERE u.status = 'ARCHIVED' AND u.archived_at IS NOT NULL
        GROUP BY u.account_id
      )

      SELECT
        u.account_id,
        COUNT(DISTINCT c.id) as coupons_redeemed
      FROM promos.coupons c
      JOIN units.units u ON u.id = c.unit_id
      INNER JOIN accounts.accounts a ON u.account_id = a.id
      LEFT JOIN account_unit_archive_dates aad ON a.id = aad.account_id
      WHERE c.is_redeemed = TRUE
        AND DATE(c.redeemed_at) = DATE('${date}')
        AND a.launched_at IS NOT NULL
        AND DATE(a.launched_at) <= DATE('${date}')
        AND (
          COALESCE(a.archived_at, aad.earliest_unit_archived_at) IS NULL
          OR DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)) > LAST_DAY(DATE('${date}'))
        )
      GROUP BY u.account_id
      HAVING coupons_redeemed > 0
      ORDER BY u.account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found coupons data for ${rows.length} eligible accounts from BigQuery`);
    return rows;
  }

  async getSubsDataFromBigQuery(date) {
    console.log(`👥 Fetching subscriber data from BigQuery for ${date}...`);

    const query = `
      WITH account_unit_archive_dates AS (
        SELECT
          u.account_id,
          MIN(u.archived_at) as earliest_unit_archived_at
        FROM units.units u
        WHERE u.status = 'ARCHIVED' AND u.archived_at IS NOT NULL
        GROUP BY u.account_id
      )

      SELECT
        u.account_id,
        COUNT(DISTINCT s.id) as active_subs_cnt
      FROM public.subscriptions s
      JOIN units.units u ON s.channel_id = u.id
      INNER JOIN accounts.accounts a ON u.account_id = a.id
      LEFT JOIN account_unit_archive_dates aad ON a.id = aad.account_id
      WHERE DATE(s.created_at) <= DATE('${date}')
        AND (s.deactivated_at IS NULL OR DATE(s.deactivated_at) > DATE('${date}'))
        AND a.launched_at IS NOT NULL
        AND DATE(a.launched_at) <= DATE('${date}')
        AND (
          COALESCE(a.archived_at, aad.earliest_unit_archived_at) IS NULL
          OR DATE(COALESCE(a.archived_at, aad.earliest_unit_archived_at)) > LAST_DAY(DATE('${date}'))
        )
      GROUP BY u.account_id
      HAVING active_subs_cnt > 0
      ORDER BY u.account_id
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    console.log(`✅ Found subscriber data for ${rows.length} eligible accounts from BigQuery`);
    return rows;
  }

  async getDatabaseDataForDate(date) {
    console.log(`🗄️  Fetching stored data from database for ${date}...`);
    const db = await this.getDatabase();

    const rows = await db.all(`
      SELECT
        account_id,
        total_spend,
        total_texts_delivered,
        coupons_redeemed,
        active_subs_cnt
      FROM daily_metrics
      WHERE date = ?
      ORDER BY account_id
    `, [date]);

    await db.close();
    console.log(`✅ Found database data for ${rows.length} accounts`);
    return rows;
  }

  compareResults(bigQueryData, dbData, metric, date) {
    console.log(`\n🔍 Comparing ${metric} data for ${date}:`);

    // Create maps for easy comparison
    const bqMap = new Map();
    const dbMap = new Map();

    bigQueryData.forEach(row => {
      bqMap.set(row.account_id, row);
    });

    dbData.forEach(row => {
      dbMap.set(row.account_id, row);
    });

    // Find accounts in BigQuery but not in DB
    const missingInDb = [];
    bqMap.forEach((bqRow, accountId) => {
      if (!dbMap.has(accountId)) {
        missingInDb.push(accountId);
      }
    });

    // Find accounts in DB but not in BigQuery
    const extraInDb = [];
    dbMap.forEach((dbRow, accountId) => {
      if (!bqMap.has(accountId)) {
        extraInDb.push(accountId);
      }
    });

    // Find value differences
    const valueDifferences = [];
    bqMap.forEach((bqRow, accountId) => {
      const dbRow = dbMap.get(accountId);
      if (dbRow) {
        const bqValue = parseFloat(bqRow[metric] || 0);
        const dbValue = parseFloat(dbRow[metric] || 0);
        if (Math.abs(bqValue - dbValue) > 0.01) { // Allow for small floating point differences
          valueDifferences.push({
            account_id: accountId,
            bigquery_value: bqValue,
            database_value: dbValue,
            difference: bqValue - dbValue
          });
        }
      }
    });

    console.log(`   BigQuery records: ${bqMap.size}`);
    console.log(`   Database records: ${dbMap.size}`);
    console.log(`   Missing in DB: ${missingInDb.length} accounts`);
    console.log(`   Extra in DB: ${extraInDb.length} accounts`);
    console.log(`   Value differences: ${valueDifferences.length} accounts`);

    if (missingInDb.length > 0) {
      console.log(`   Missing accounts: ${missingInDb.slice(0, 5).join(', ')}${missingInDb.length > 5 ? '...' : ''}`);
    }

    if (extraInDb.length > 0) {
      console.log(`   Extra accounts: ${extraInDb.slice(0, 5).join(', ')}${extraInDb.length > 5 ? '...' : ''}`);
    }

    if (valueDifferences.length > 0) {
      console.log(`   Value differences (first 3):`);
      valueDifferences.slice(0, 3).forEach(diff => {
        console.log(`     ${diff.account_id}: BQ=${diff.bigquery_value}, DB=${diff.database_value}, Diff=${diff.difference}`);
      });
    }

    return {
      bigQueryCount: bqMap.size,
      databaseCount: dbMap.size,
      missingInDb: missingInDb.length,
      extraInDb: extraInDb.length,
      valueDifferences: valueDifferences.length,
      perfect: missingInDb.length === 0 && extraInDb.length === 0 && valueDifferences.length === 0
    };
  }

  async testDate(date) {
    console.log(`\n🎯 Testing ETL accuracy for ${date}`);
    console.log('='.repeat(50));

    // Get BigQuery data for all metrics
    const [spendData, textsData, couponsData, subsData] = await Promise.all([
      this.getSpendDataFromBigQuery(date),
      this.getTextsDataFromBigQuery(date),
      this.getCouponsDataFromBigQuery(date),
      this.getSubsDataFromBigQuery(date)
    ]);

    // Get database data
    const dbData = await this.getDatabaseDataForDate(date);

    // Compare each metric
    const spendComparison = this.compareResults(spendData, dbData, 'total_spend', date);
    const textsComparison = this.compareResults(textsData, dbData, 'total_texts_delivered', date);
    const couponsComparison = this.compareResults(couponsData, dbData, 'coupons_redeemed', date);
    const subsComparison = this.compareResults(subsData, dbData, 'active_subs_cnt', date);

    console.log(`\n📊 Summary for ${date}:`);
    console.log(`   Spend: ${spendComparison.perfect ? '✅ Perfect' : '❌ Issues found'}`);
    console.log(`   Texts: ${textsComparison.perfect ? '✅ Perfect' : '❌ Issues found'}`);
    console.log(`   Coupons: ${couponsComparison.perfect ? '✅ Perfect' : '❌ Issues found'}`);
    console.log(`   Subscribers: ${subsComparison.perfect ? '✅ Perfect' : '❌ Issues found'}`);

    const overallPerfect = spendComparison.perfect && textsComparison.perfect &&
                          couponsComparison.perfect && subsComparison.perfect;

    return {
      date,
      overallPerfect,
      spend: spendComparison,
      texts: textsComparison,
      coupons: couponsComparison,
      subs: subsComparison
    };
  }

  async runComparison(dates) {
    console.log('🚀 Starting BigQuery vs Database comparison');
    console.log('Checking eligibility criteria and data consistency...\n');

    const results = [];
    for (const date of dates) {
      const result = await this.testDate(date);
      results.push(result);
    }

    console.log('\n' + '='.repeat(60));
    console.log('🎯 FINAL SUMMARY');
    console.log('='.repeat(60));

    results.forEach(result => {
      console.log(`${result.date}: ${result.overallPerfect ? '✅ PERFECT MATCH' : '❌ DISCREPANCIES FOUND'}`);
    });

    const allPerfect = results.every(r => r.overallPerfect);
    console.log(`\nOverall Status: ${allPerfect ? '✅ All dates match perfectly' : '❌ Some discrepancies found'}`);

    return results;
  }
}

// Run comparison for the three dates Tyler is concerned about
const dates = ['2025-09-16', '2025-09-17', '2025-09-18'];
const comparator = new BigQueryETLComparison();
comparator.runComparison(dates).catch(console.error);