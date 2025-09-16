import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import dotenv from 'dotenv';

dotenv.config();

export const getDatabasePath = () => {
  return process.env.SQLITE_DB_PATH || './data/churnguard_simulation.db';
};

export const getDatabase = async () => {
  const db = await open({
    filename: getDatabasePath(),
    driver: sqlite3.Database
  });
  return db;
};

// Singleton pattern for shared database connection
let dbInstance = null;

export const getSharedDatabase = async () => {
  if (!dbInstance) {
    dbInstance = await getDatabase();
  }
  return dbInstance;
};