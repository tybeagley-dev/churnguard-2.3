import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';

// Import our organized modules
import { getSharedDatabase } from './config/database.js';
import historicalPerformanceRoutes from './src/routes/historical-performance.routes.js';
import monthlyTrendsRoutes from './src/routes/monthly-trends.routes.js';
import weeklyViewRoutes from './src/routes/weekly-view.routes.js';

const app = express();
const port = process.env.PORT || 3003;

// Get current directory for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'dist')));

// Initialize database connection
let db;
try {
  db = await getSharedDatabase();
  console.log('📊 Connected to SQLite simulation database');
} catch (error) {
  console.error('❌ Database connection failed:', error);
  process.exit(1);
}

// Basic health check
app.get('/api/test', (req, res) => {
  res.json({ message: 'ChurnGuard 2.3 Clean API - Server is running!' });
});

// Mount route modules
app.use('/api', historicalPerformanceRoutes);
app.use('/api', monthlyTrendsRoutes);
app.use('/api', weeklyViewRoutes);

// Serve frontend for all other routes
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});

// Start server
app.listen(port, () => {
  console.log(`🚀 ChurnGuard 2.3 (Clean Architecture) running at http://localhost:${port}`);
  console.log('📊 Serving data from SQLite simulation database');
  console.log('🎯 Clean, organized, production-ready!');
});

export default app;