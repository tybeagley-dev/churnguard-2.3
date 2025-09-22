import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import pkg from 'pg';
import dotenv from 'dotenv';

const { Pool } = pkg;
dotenv.config();

export const getDatabasePath = () => {
  return process.env.SQLITE_DB_PATH || './data/churnguard_simulation.db';
};

export const getDatabase = async () => {
  if (process.env.DATABASE_URL) {
    // Use PostgreSQL for production
    const pool = new Pool({
      connectionString: process.env.DATABASE_URL,
    });
    return pool;
  } else {
    // Use SQLite for development
    const db = await open({
      filename: getDatabasePath(),
      driver: sqlite3.Database
    });
    return db;
  }
};

// Singleton pattern for shared database connection
let dbInstance = null;

export const getSharedDatabase = async () => {
  if (!dbInstance) {
    dbInstance = await getDatabase();
  }
  return dbInstance;
};