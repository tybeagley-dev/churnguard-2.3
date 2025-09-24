import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

class ETLTracker {
  constructor() {
    if (!process.env.DATABASE_URL) {
      throw new Error('DATABASE_URL environment variable is required');
    }

    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false,
      connectionTimeoutMillis: 30000,
      idleTimeoutMillis: 300000,
      max: 5
    });
  }

  async setupTrackingTable() {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS etl_runs (
        date TEXT,
        step TEXT, -- 'accounts', 'daily', 'monthly'
        status TEXT, -- 'running', 'completed', 'failed'
        started_at TIMESTAMPTZ DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        error_message TEXT,
        metadata JSONB,
        PRIMARY KEY (date, step)
      );
    `);

    // Create index for status queries
    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_etl_runs_status ON etl_runs(date, status);
    `);
  }

  async startStep(date, step, metadata = {}) {
    await this.setupTrackingTable();

    const result = await this.pool.query(`
      INSERT INTO etl_runs (date, step, status, started_at, metadata)
      VALUES ($1, $2, 'running', NOW(), $3)
      ON CONFLICT (date, step) DO UPDATE SET
        status = 'running',
        started_at = NOW(),
        completed_at = NULL,
        error_message = NULL,
        metadata = EXCLUDED.metadata
      RETURNING *
    `, [date, step, JSON.stringify(metadata)]);

    console.log(`🚀 Started ETL step: ${step} for ${date}`);
    return result.rows[0];
  }

  async completeStep(date, step, metadata = {}) {
    const result = await this.pool.query(`
      UPDATE etl_runs
      SET status = 'completed', completed_at = NOW(), metadata = $3
      WHERE date = $1 AND step = $2
      RETURNING *
    `, [date, step, JSON.stringify(metadata)]);

    console.log(`✅ Completed ETL step: ${step} for ${date}`);
    return result.rows[0];
  }

  async failStep(date, step, errorMessage, metadata = {}) {
    const result = await this.pool.query(`
      UPDATE etl_runs
      SET status = 'failed', completed_at = NOW(), error_message = $3, metadata = $4
      WHERE date = $1 AND step = $2
      RETURNING *
    `, [date, step, errorMessage, JSON.stringify(metadata)]);

    console.log(`❌ Failed ETL step: ${step} for ${date} - ${errorMessage}`);
    return result.rows[0];
  }

  async getStepStatus(date, step) {
    await this.setupTrackingTable();

    const result = await this.pool.query(`
      SELECT * FROM etl_runs WHERE date = $1 AND step = $2
    `, [date, step]);

    return result.rows[0] || null;
  }

  async getDateStatus(date) {
    await this.setupTrackingTable();

    const result = await this.pool.query(`
      SELECT * FROM etl_runs WHERE date = $1 ORDER BY step
    `, [date]);

    return result.rows;
  }

  async isStepComplete(date, step) {
    const status = await this.getStepStatus(date, step);
    return status && status.status === 'completed';
  }

  async arePrerequisitesComplete(date, step) {
    switch (step) {
      case 'accounts':
        return true; // No prerequisites
      case 'daily':
        return await this.isStepComplete(date, 'accounts');
      case 'monthly':
        return await this.isStepComplete(date, 'accounts') &&
               await this.isStepComplete(date, 'daily');
      default:
        throw new Error(`Unknown ETL step: ${step}`);
    }
  }

  async validateCanRun(date, step) {
    const prerequisitesComplete = await this.arePrerequisitesComplete(date, step);
    if (!prerequisitesComplete) {
      const missing = [];
      if (step !== 'accounts' && !await this.isStepComplete(date, 'accounts')) {
        missing.push('accounts');
      }
      if (step === 'monthly' && !await this.isStepComplete(date, 'daily')) {
        missing.push('daily');
      }
      throw new Error(`Prerequisites not complete for ${step}. Missing: ${missing.join(', ')}`);
    }

    // Check if step is already running
    const currentStatus = await this.getStepStatus(date, step);
    if (currentStatus && currentStatus.status === 'running') {
      throw new Error(`ETL step ${step} is already running for ${date}`);
    }

    return true;
  }
}

export { ETLTracker };