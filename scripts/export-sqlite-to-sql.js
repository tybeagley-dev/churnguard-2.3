import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import fs from 'fs';

class SQLiteToSQLExporter {
  constructor() {
    this.sqliteDbPath = './data/churnguard_simulation.db';
    this.outputFile = './scripts/postgres-data.sql';
  }

  async connectSQLite() {
    console.log('📂 Connecting to SQLite database...');
    return await open({
      filename: this.sqliteDbPath,
      driver: sqlite3.Database
    });
  }

  escapeString(str) {
    if (str === null || str === undefined) return 'NULL';
    if (typeof str === 'number') return str;
    return "'" + str.toString().replace(/'/g, "''") + "'";
  }

  async exportAccounts() {
    console.log('👥 Exporting accounts...');
    const sqlite = await this.connectSQLite();
    const accounts = await sqlite.all('SELECT * FROM accounts');
    console.log(`📊 Found ${accounts.length} accounts`);

    let sql = "-- Accounts data\nDELETE FROM accounts;\n\n";

    for (const account of accounts) {
      sql += `INSERT INTO accounts (account_id, account_name, status, launched_at, csm_owner, hubspot_id, archived_at, earliest_unit_archived_at, last_updated) VALUES (${this.escapeString(account.account_id)}, ${this.escapeString(account.account_name)}, ${this.escapeString(account.status)}, ${this.escapeString(account.launched_at)}, ${this.escapeString(account.csm_owner)}, ${this.escapeString(account.hubspot_id)}, ${this.escapeString(account.archived_at)}, ${this.escapeString(account.earliest_unit_archived_at)}, ${this.escapeString(account.last_updated)});\n`;
    }

    await sqlite.close();
    return sql + "\n";
  }

  async exportDailyMetrics() {
    console.log('📈 Exporting daily metrics...');
    const sqlite = await this.connectSQLite();
    const count = await sqlite.get('SELECT COUNT(*) as count FROM daily_metrics');
    console.log(`📊 Found ${count.count} daily metrics records`);

    let sql = "-- Daily metrics data\nDELETE FROM daily_metrics;\n\n";

    // Export in chunks
    const chunkSize = 1000;
    let offset = 0;

    while (true) {
      const metrics = await sqlite.all(`
        SELECT dm.* FROM daily_metrics dm
        INNER JOIN accounts a ON dm.account_id = a.account_id
        ORDER BY dm.account_id, dm.date
        LIMIT ${chunkSize} OFFSET ${offset}
      `);

      if (metrics.length === 0) break;

      for (const metric of metrics) {
        sql += `INSERT INTO daily_metrics (account_id, date, total_spend, total_texts_delivered, coupons_redeemed, active_subs_cnt, spend_updated_at, texts_updated_at, coupons_updated_at, subs_updated_at) VALUES (${this.escapeString(metric.account_id)}, ${this.escapeString(metric.date)}, ${metric.total_spend || 0}, ${metric.total_texts_delivered || 0}, ${metric.coupons_redeemed || 0}, ${metric.active_subs_cnt || 0}, ${this.escapeString(metric.spend_updated_at)}, ${this.escapeString(metric.texts_updated_at)}, ${this.escapeString(metric.coupons_updated_at)}, ${this.escapeString(metric.subs_updated_at)});\n`;
      }

      offset += chunkSize;
      console.log(`   ✅ Exported ${Math.min(offset, count.count)}/${count.count} daily metrics`);
    }

    await sqlite.close();
    return sql + "\n";
  }

  async exportMonthlyMetrics() {
    console.log('📅 Exporting monthly metrics...');
    const sqlite = await this.connectSQLite();
    const monthlyMetrics = await sqlite.all('SELECT * FROM monthly_metrics ORDER BY account_id, month');
    console.log(`📊 Found ${monthlyMetrics.length} monthly metrics records`);

    let sql = "-- Monthly metrics data\nDELETE FROM monthly_metrics;\n\n";

    for (const metric of monthlyMetrics) {
      sql += `INSERT INTO monthly_metrics (account_id, month, month_label, total_spend, total_texts_delivered, total_coupons_redeemed, avg_active_subs_cnt, trending_risk_level, trending_risk_reasons, historical_risk_level, risk_reasons, created_at, updated_at) VALUES (${this.escapeString(metric.account_id)}, ${this.escapeString(metric.month)}, ${this.escapeString(metric.month_label)}, ${metric.total_spend || 0}, ${metric.total_texts_delivered || 0}, ${metric.total_coupons_redeemed || 0}, ${metric.avg_active_subs_cnt || 0}, ${this.escapeString(metric.trending_risk_level)}, ${this.escapeString(metric.trending_risk_reasons)}, ${this.escapeString(metric.historical_risk_level)}, ${this.escapeString(metric.risk_reasons)}, ${this.escapeString(metric.created_at)}, ${this.escapeString(metric.updated_at)});\n`;
    }

    await sqlite.close();
    return sql + "\n";
  }

  async export() {
    try {
      console.log('🚀 Starting SQLite data export...');
      console.log('======================================');

      const accountsSQL = await this.exportAccounts();
      const dailySQL = await this.exportDailyMetrics();
      const monthlySQL = await this.exportMonthlyMetrics();

      const fullSQL = `-- ChurnGuard Data Export
-- Generated: ${new Date().toISOString()}

${accountsSQL}${dailySQL}${monthlySQL}`;

      fs.writeFileSync(this.outputFile, fullSQL);

      console.log('======================================');
      console.log(`🎉 Export completed successfully!`);
      console.log(`📄 SQL file written to: ${this.outputFile}`);

      return {
        success: true,
        outputFile: this.outputFile,
        size: Math.round(fullSQL.length / 1024) + ' KB'
      };

    } catch (error) {
      console.error('❌ Export failed:', error);
      throw error;
    }
  }
}

// Run export if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const exporter = new SQLiteToSQLExporter();
  exporter.export()
    .then(result => {
      console.log('✅ Export script completed successfully');
      process.exit(0);
    })
    .catch(error => {
      console.error('❌ Export script failed:', error);
      process.exit(1);
    });
}

export { SQLiteToSQLExporter };