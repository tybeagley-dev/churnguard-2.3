import { BigQuery } from '@google-cloud/bigquery';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

class RevenueComparison {
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

  async getBigQueryRevenue(date) {
    console.log(`💰 Getting BigQuery revenue for ${date}...`);

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
        SUM(tr.total) as total_revenue,
        COUNT(DISTINCT tr.account_id) as account_count
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
        AND tr.total > 0
    `;

    const [rows] = await this.bigquery.query({ query, location: 'US' });
    const result = rows[0];

    console.log(`✅ BigQuery: $${result.total_revenue?.toFixed(2) || '0.00'} from ${result.account_count || 0} accounts`);
    return {
      total: parseFloat(result.total_revenue || 0),
      accounts: parseInt(result.account_count || 0)
    };
  }

  async getDatabaseRevenue(date) {
    console.log(`🗄️  Getting database revenue for ${date}...`);
    const db = await this.getDatabase();

    const result = await db.get(`
      SELECT
        SUM(total_spend) as total_revenue,
        COUNT(*) as account_count,
        COUNT(CASE WHEN total_spend > 0 THEN 1 END) as accounts_with_spend
      FROM daily_metrics
      WHERE date = ?
    `, [date]);

    await db.close();

    console.log(`✅ Database: $${result.total_revenue?.toFixed(2) || '0.00'} from ${result.accounts_with_spend || 0} accounts (${result.account_count} total records)`);
    return {
      total: parseFloat(result.total_revenue || 0),
      accounts: parseInt(result.accounts_with_spend || 0),
      totalRecords: parseInt(result.account_count || 0)
    };
  }

  async compareRevenue(date) {
    console.log(`\n📊 Revenue Comparison for ${date}`);
    console.log('='.repeat(50));

    const [bqRevenue, dbRevenue] = await Promise.all([
      this.getBigQueryRevenue(date),
      this.getDatabaseRevenue(date)
    ]);

    const difference = dbRevenue.total - bqRevenue.total;
    const percentDiff = bqRevenue.total > 0 ? (difference / bqRevenue.total) * 100 : 0;

    console.log(`\n💵 Revenue Summary:`);
    console.log(`   BigQuery:  $${bqRevenue.total.toFixed(2)} (${bqRevenue.accounts} accounts)`);
    console.log(`   Database:  $${dbRevenue.total.toFixed(2)} (${dbRevenue.accounts} accounts with spend, ${dbRevenue.totalRecords} total records)`);
    console.log(`   Difference: $${difference.toFixed(2)} (${percentDiff.toFixed(2)}%)`);
    console.log(`   Status: ${Math.abs(difference) < 0.01 ? '✅ Match' : difference > 0 ? '⬆️ Database Higher' : '⬇️ Database Lower'}`);

    return {
      date,
      bigquery: bqRevenue.total,
      database: dbRevenue.total,
      difference,
      percentDiff,
      match: Math.abs(difference) < 0.01
    };
  }

  async runComparison(dates) {
    console.log('💰 Starting Revenue Comparison Analysis\n');

    const results = [];
    for (const date of dates) {
      const result = await this.compareRevenue(date);
      results.push(result);
    }

    console.log('\n' + '='.repeat(60));
    console.log('💰 REVENUE SUMMARY');
    console.log('='.repeat(60));

    let totalBqRevenue = 0;
    let totalDbRevenue = 0;

    results.forEach(result => {
      totalBqRevenue += result.bigquery;
      totalDbRevenue += result.database;
      console.log(`${result.date}: BQ=$${result.bigquery.toFixed(2)}, DB=$${result.database.toFixed(2)}, Diff=$${result.difference.toFixed(2)} (${result.percentDiff.toFixed(2)}%)`);
    });

    const totalDifference = totalDbRevenue - totalBqRevenue;
    const totalPercentDiff = totalBqRevenue > 0 ? (totalDifference / totalBqRevenue) * 100 : 0;

    console.log('\n📈 Three-Day Totals:');
    console.log(`   BigQuery Total:  $${totalBqRevenue.toFixed(2)}`);
    console.log(`   Database Total:  $${totalDbRevenue.toFixed(2)}`);
    console.log(`   Total Difference: $${totalDifference.toFixed(2)} (${totalPercentDiff.toFixed(2)}%)`);

    return results;
  }
}

// Run revenue comparison for the three dates
const dates = ['2025-09-16', '2025-09-17', '2025-09-18'];
const comparator = new RevenueComparison();
comparator.runComparison(dates).catch(console.error);