import { BigQuery } from '@google-cloud/bigquery';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

class SeptemberMTDAnalysis {
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

  async getSeptemberDatesFromDB() {
    const db = await this.getDatabase();
    const rows = await db.all(`
      SELECT DISTINCT date
      FROM daily_metrics
      WHERE date LIKE '2025-09%'
      ORDER BY date
    `);
    await db.close();
    return rows.map(r => r.date);
  }

  async getBigQueryRevenueForDate(date) {
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

    return {
      date,
      total: parseFloat(result.total_revenue || 0),
      accounts: parseInt(result.account_count || 0)
    };
  }

  async getDatabaseRevenueForDate(date) {
    const db = await this.getDatabase();
    const result = await db.get(`
      SELECT
        SUM(total_spend) as total_revenue,
        COUNT(*) as total_records,
        COUNT(CASE WHEN total_spend > 0 THEN 1 END) as accounts_with_spend
      FROM daily_metrics
      WHERE date = ?
    `, [date]);
    await db.close();

    return {
      date,
      total: parseFloat(result.total_revenue || 0),
      accounts: parseInt(result.accounts_with_spend || 0),
      totalRecords: parseInt(result.total_records || 0)
    };
  }

  async analyzeSingleDate(date) {
    console.log(`📊 Analyzing ${date}...`);

    try {
      const [bqData, dbData] = await Promise.all([
        this.getBigQueryRevenueForDate(date),
        this.getDatabaseRevenueForDate(date)
      ]);

      const difference = dbData.total - bqData.total;
      const percentDiff = bqData.total > 0 ? (difference / bqData.total) * 100 : 0;
      const accountDiff = dbData.accounts - bqData.accounts;
      const phantomRecords = dbData.totalRecords - dbData.accounts;

      return {
        date,
        bigquery: {
          revenue: bqData.total,
          accounts: bqData.accounts
        },
        database: {
          revenue: dbData.total,
          accounts: dbData.accounts,
          totalRecords: dbData.totalRecords,
          phantomRecords
        },
        differences: {
          revenue: difference,
          revenuePercent: percentDiff,
          accounts: accountDiff
        },
        status: Math.abs(difference) < 0.01 ? 'MATCH' : (difference > 0 ? 'DB_HIGHER' : 'BQ_HIGHER')
      };
    } catch (error) {
      console.log(`   ❌ Error: ${error.message}`);
      return {
        date,
        error: error.message,
        status: 'ERROR'
      };
    }
  }

  async runFullAnalysis() {
    console.log('🚀 Starting September 2025 MTD Revenue Analysis');
    console.log('=' .repeat(70));

    const dates = await this.getSeptemberDatesFromDB();
    console.log(`📅 Found ${dates.length} days of data in September`);

    if (dates.length > 0) {
      const missingDates = [];
      for (let day = 1; day <= 30; day++) {
        const testDate = `2025-09-${day.toString().padStart(2, '0')}`;
        if (!dates.includes(testDate)) {
          missingDates.push(testDate);
        }
      }
      if (missingDates.length > 0) {
        console.log(`⚠️  Missing dates: ${missingDates.join(', ')}`);
      }
    }

    console.log('\n🔍 Running daily comparisons...\n');

    const results = [];
    for (const date of dates) {
      const result = await this.analyzeSingleDate(date);
      results.push(result);

      if (result.status !== 'ERROR') {
        const statusIcon = result.status === 'MATCH' ? '✅' :
                          result.status === 'DB_HIGHER' ? '⬆️' : '⬇️';
        console.log(`${date}: ${statusIcon} BQ=$${result.bigquery.revenue.toFixed(2)} | DB=$${result.database.revenue.toFixed(2)} | Diff=$${result.differences.revenue.toFixed(2)} (${result.differences.revenuePercent.toFixed(2)}%) | Phantom=${result.database.phantomRecords}`);
      }
    }

    // Calculate totals
    const validResults = results.filter(r => r.status !== 'ERROR');
    const totalBQ = validResults.reduce((sum, r) => sum + r.bigquery.revenue, 0);
    const totalDB = validResults.reduce((sum, r) => sum + r.database.revenue, 0);
    const totalDiff = totalDB - totalBQ;
    const totalPercentDiff = totalBQ > 0 ? (totalDiff / totalBQ) * 100 : 0;
    const totalPhantomRecords = validResults.reduce((sum, r) => sum + r.database.phantomRecords, 0);

    console.log('\n' + '=' .repeat(70));
    console.log('📈 SEPTEMBER 2025 MTD SUMMARY');
    console.log('=' .repeat(70));
    console.log(`📅 Days analyzed: ${validResults.length}`);
    console.log(`💰 BigQuery Total: $${totalBQ.toLocaleString('en-US', {minimumFractionDigits: 2})}`);
    console.log(`💰 Database Total: $${totalDB.toLocaleString('en-US', {minimumFractionDigits: 2})}`);
    console.log(`📊 Total Difference: $${totalDiff.toFixed(2)} (${totalPercentDiff.toFixed(2)}%)`);
    console.log(`👻 Total Phantom Records: ${totalPhantomRecords.toLocaleString()}`);

    // Identify problem days
    const problemDays = validResults.filter(r =>
      Math.abs(r.differences.revenue) > 1.00 || r.database.phantomRecords > 100
    );

    if (problemDays.length > 0) {
      console.log(`\n🚨 Problem Days (>$1 diff or >100 phantom records):`);
      problemDays.forEach(day => {
        console.log(`   ${day.date}: $${day.differences.revenue.toFixed(2)} diff, ${day.database.phantomRecords} phantom records`);
      });
    }

    // Check for systematic patterns
    const recentDays = validResults.slice(-7); // Last 7 days
    const recentPhantomAvg = recentDays.reduce((sum, r) => sum + r.database.phantomRecords, 0) / recentDays.length;
    const earlyDays = validResults.slice(0, 7); // First 7 days
    const earlyPhantomAvg = earlyDays.reduce((sum, r) => sum + r.database.phantomRecords, 0) / earlyDays.length;

    console.log(`\n🔍 Pattern Analysis:`);
    console.log(`   Early Sept phantom avg: ${earlyPhantomAvg.toFixed(0)} records/day`);
    console.log(`   Recent phantom avg: ${recentPhantomAvg.toFixed(0)} records/day`);
    console.log(`   Trend: ${recentPhantomAvg > earlyPhantomAvg ? '📈 Worsening' : '📉 Improving'}`);

    return {
      totalResults: validResults.length,
      totalBQ,
      totalDB,
      totalDiff,
      totalPercentDiff,
      totalPhantomRecords,
      problemDays: problemDays.length,
      results: validResults
    };
  }
}

// Execute the analysis
const analyzer = new SeptemberMTDAnalysis();
analyzer.runFullAnalysis().catch(console.error);