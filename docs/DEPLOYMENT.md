# ChurnGuard 2.3 Clean Server Deployment Guide

## 🚀 Quick Start - Clean Architecture

The clean server (`server-clean.js`) is production-ready and eliminates all bloat from the legacy server.

### Local Development
```bash
# Start clean server only
npm run server:clean

# Start clean server + frontend development
npm run dev:clean
```

### Production Deployment
```bash
# Build and start production server
npm run production:clean

# Or run individually:
npm run build
npm run start:clean
```

## ✅ What's Working

- **Database**: SQLite connection established (`./data/churnguard_simulation.db`)
- **API Endpoints**: All routes functional
  - `/api/test` - Health check
  - `/api/accounts` - Account data
  - `/api/monthly-trends` - Trend analytics
  - `/api/weekly-view` - Weekly metrics
  - `/api/historical-performance` - Historical data
- **Frontend**: Vite-built React app served from `/dist`
- **Architecture**: Clean MVC structure with organized routes and controllers

## 🔧 Configuration

- **Default Port**: 3003 (configurable via `PORT` environment variable)
- **Database Path**: Configurable via `SQLITE_DB_PATH` environment variable
- **Environment**: Uses `.env` file for configuration

## 📁 Clean Architecture Structure

```
src/
├── routes/           # API route definitions
├── controllers/      # Business logic handlers
config/
├── database.js       # Database connection management
```

## 🎯 Ready for Live Deployment

The clean server has been tested and verified:
- Database connectivity ✅
- API functionality ✅
- Frontend serving ✅
- Clean architecture ✅
- No bloat or legacy code ✅

This version is ready to replace the legacy server for live deployment.