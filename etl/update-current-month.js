import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

class CurrentMonthUpdater {
  constructor() {}

  async initDatabase() {
    this.db = await open({
      filename: process.env.SQLITE_DB_PATH || './data/churnguard_simulation.db',
      driver: sqlite3.Database
    });
  }

  async updateCurrentMonth() {
    if (!this.db) await this.initDatabase();

    const currentMonth = new Date().toISOString().slice(0, 7); // YYYY-MM format
    const monthLabel = new Date(currentMonth + '-01').toLocaleDateString('en-US', { year: 'numeric', month: 'long', timeZone: 'UTC' });

    console.log(`🔄 Updating current month data for ${monthLabel}...`);

    // Delete existing current month data
    await this.db.run(
      "DELETE FROM monthly_metrics WHERE month = ?",
      currentMonth
    );

    // Recreate current month data from daily_metrics
    const result = await this.db.run(`
      INSERT INTO monthly_metrics (
        account_id, month, month_label, total_spend, total_texts_delivered,
        total_coupons_redeemed, avg_active_subs_cnt, days_with_activity,
        last_updated
      )
      SELECT 
        dm.account_id,
        ? as month,
        ? as month_label,
        SUM(dm.total_spend) as total_spend,
        SUM(dm.total_texts_delivered) as total_texts_delivered,
        SUM(dm.coupons_redeemed) as total_coupons_redeemed,
        ROUND(AVG(dm.active_subs_cnt)) as avg_active_subs_cnt,
        COUNT(DISTINCT dm.date) as days_with_activity,
        datetime('now') as last_updated
      FROM accounts a
      LEFT JOIN daily_metrics dm ON a.account_id = dm.account_id 
        AND strftime('%Y-%m', dm.date) = ?
      WHERE (
        -- Apply same filtering as other views
        DATE(a.launched_at) <= DATE(? || '-01', '+1 month', '-1 day')
        AND (
          (a.archived_at IS NULL AND a.earliest_unit_archived_at IS NULL)
          OR DATE(COALESCE(a.archived_at, a.earliest_unit_archived_at)) >= DATE(? || '-01')
        )
      )
      GROUP BY a.account_id
    `, currentMonth, monthLabel, currentMonth, currentMonth, currentMonth);

    console.log(`✅ Updated ${result.changes} accounts for ${monthLabel}`);
    
    await this.db.close();
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const updater = new CurrentMonthUpdater();
  updater.updateCurrentMonth().catch(console.error);
}

export { CurrentMonthUpdater };