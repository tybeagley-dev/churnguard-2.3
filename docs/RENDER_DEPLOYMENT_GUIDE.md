# ChurnGuard 2.3 - Complete Render Deployment Guide

**🚀 Step-by-Step Production Deployment to Render with PostgreSQL**

*Last Updated: September 19, 2025*
*Target Environment: Render.com with PostgreSQL Database*

---

## 📋 Pre-Deployment Checklist

### Prerequisites
- [ ] Render.com account created
- [ ] GitHub repository with ChurnGuard 2.3 code
- [ ] Google Cloud Project with BigQuery access
- [ ] BigQuery service account credentials (JSON file)
- [ ] Domain name ready (optional but recommended)

### Code Preparation
- [ ] Latest code pushed to GitHub main branch
- [ ] All SQLite-specific code updated for PostgreSQL (see Migration Plan)
- [ ] Environment variables documented
- [ ] Build process tested locally: `npm run build`

---

## 🎯 Phase 1: Render Account & Service Setup

### Step 1.1: Create Render Account
1. Go to [render.com](https://render.com)
2. Sign up with GitHub account (recommended for easier repository access)
3. Verify email address
4. Complete account setup

### Step 1.2: Connect GitHub Repository
1. In Render Dashboard → **Settings** → **Git Providers**
2. Click **Connect GitHub**
3. Authorize Render to access your repositories
4. Locate your ChurnGuard 2.3 repository

---

## 🗄️ Phase 2: PostgreSQL Database Setup

### Step 2.1: Create PostgreSQL Database
1. In Render Dashboard → **New** → **PostgreSQL**
2. Configure database settings:
   ```
   Name: churnguard-production-db
   Database: churnguard_prod
   User: (auto-generated, note for later)
   Region: Choose closest to your users
   PostgreSQL Version: 15 (recommended)
   Plan: Select based on data size (start with Starter $7/month)
   ```
3. Click **Create Database**
4. **IMPORTANT**: Save the connection details shown after creation

### Step 2.2: Note Database Connection Info
After database creation, you'll see:
```
Internal Database URL: postgresql://user:password@dpg-xxxxx-a.oregon-postgres.render.com/database_internal
External Database URL: postgresql://user:password@dpg-xxxxx-a.oregon-postgres.render.com/database
```

**Copy both URLs** - you'll need the External URL for your application.

### Step 2.3: Test Database Connection (Optional)
Using a PostgreSQL client (like pgAdmin or command line):
```bash
psql "postgresql://your-external-database-url"
```
Should connect successfully and show empty database.

---

## 🔧 Phase 3: Environment Variables Preparation

### Step 3.1: Gather Required Environment Variables
Create a secure note with these values:

```bash
# Database Configuration
DATABASE_URL=postgresql://user:password@host:5432/database

# BigQuery Configuration
GOOGLE_CLOUD_PROJECT_ID=your-project-id
GOOGLE_APPLICATION_CREDENTIALS_JSON={"type":"service_account",...}

# Server Configuration
NODE_ENV=production
PORT=10000

# Simulation Configuration (for historical data)
SIMULATION_START_DATE=2025-07-01
SIMULATION_END_DATE=2025-09-03

# Optional: HubSpot Integration
HUBSPOT_API_KEY=your-hubspot-api-key-here

# Optional: Debug Mode
DEBUG=false
```

### Step 3.2: Prepare BigQuery Credentials
1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Navigate to **IAM & Admin** → **Service Accounts**
3. Find your BigQuery service account
4. Click **Keys** tab → **Add Key** → **Create New Key** → **JSON**
5. Download the JSON file
6. **IMPORTANT**: Copy the entire JSON content as a single line (no line breaks)
7. This becomes your `GOOGLE_APPLICATION_CREDENTIALS_JSON` value

---

## 🚀 Phase 4: Web Service Deployment

### Step 4.1: Create Web Service
1. In Render Dashboard → **New** → **Web Service**
2. Connect to your GitHub repository
3. Configure basic settings:
   ```
   Name: churnguard-production
   Root Directory: (leave blank)
   Environment: Node
   Region: Same as your database
   Branch: main
   ```

### Step 4.2: Configure Build Settings
```bash
# Build Command
npm install && npm run build

# Start Command
npm run start:clean
```

### Step 4.3: Advanced Settings
```bash
# Node Version
18.x or higher

# Plan
Select based on expected traffic (start with Starter $7/month)

# Auto-Deploy
Enabled (deploys automatically on git push)
```

---

## 🔐 Phase 5: Environment Variables Configuration

### Step 5.1: Add Environment Variables
In your web service settings → **Environment** tab:

**Add each variable one by one:**

| Key | Value | Notes |
|-----|-------|-------|
| `DATABASE_URL` | `postgresql://user:password@host:5432/database` | From Step 2.2 |
| `GOOGLE_CLOUD_PROJECT_ID` | `your-project-id` | From Google Cloud |
| `GOOGLE_APPLICATION_CREDENTIALS_JSON` | `{"type":"service_account",...}` | Full JSON as single line |
| `NODE_ENV` | `production` | Required |
| `PORT` | `10000` | Render default |
| `SIMULATION_START_DATE` | `2025-07-01` | For data simulation |
| `SIMULATION_END_DATE` | `2025-09-03` | For data simulation |

### Step 5.2: Optional Variables
Add these if you have them configured:
- `HUBSPOT_API_KEY` (if using HubSpot integration)
- `DEBUG` (set to `false` for production)

---

## 📦 Phase 6: Database Migration & Data Setup

### Step 6.1: Deploy Application First
1. Click **Create Web Service** (this will trigger first deployment)
2. Monitor deployment logs for any errors
3. Wait for deployment to complete (usually 5-10 minutes)

### Step 6.2: Initialize Database Schema
**After successful deployment**, you need to populate your database.

**Option A: Using Render Shell (Recommended)**
1. In your web service → **Shell** tab
2. Run database initialization:
   ```bash
   # Install PostgreSQL client tools
   npm run simulate-all:prod
   ```

**Option B: Local Connection**
If you have the database URL, you can run locally:
```bash
# Set environment variable locally
export DATABASE_URL="your-postgresql-url"

# Run production simulation
npm run simulate-all:prod
```

### Step 6.3: Verify Database Population
Check that data was created:
1. Connect to your PostgreSQL database
2. Verify tables exist: `accounts`, `daily_metrics`, `monthly_metrics`
3. Check row counts: `SELECT COUNT(*) FROM accounts;`
4. Should see ~800-900 accounts if simulation ran successfully

---

## 🌐 Phase 7: Domain & SSL Setup (Optional)

### Step 7.1: Custom Domain Setup
1. In your web service → **Settings** → **Custom Domains**
2. Click **Add Custom Domain**
3. Enter your domain: `churnguard.yourdomain.com`
4. Copy the CNAME record shown
5. Add CNAME record in your DNS provider:
   ```
   CNAME: churnguard.yourdomain.com → your-app.onrender.com
   ```

### Step 7.2: SSL Certificate
- Render automatically provisions SSL certificates for custom domains
- May take 1-2 hours to activate
- Verify by visiting `https://churnguard.yourdomain.com`

---

## ✅ Phase 8: Post-Deployment Verification

### Step 8.1: Basic Health Checks
Test these URLs (replace with your actual domain):

```bash
# Health check endpoint
https://your-app.onrender.com/api/test

# Accounts data
https://your-app.onrender.com/api/accounts

# Dashboard data
https://your-app.onrender.com/api/monthly-trends
```

### Step 8.2: Frontend Verification
1. Visit your main application URL
2. Verify dashboard loads properly
3. Check that charts display data
4. Test account filtering and navigation
5. Verify all 800+ accounts are visible

### Step 8.3: Database Performance Check
Monitor these metrics for the first week:
- Response times for API endpoints
- Database connection pool usage
- Memory usage on Render
- Any error logs

---

## 🚨 Phase 9: Troubleshooting Guide

### Common Deployment Issues

#### 🔍 Build Fails
**Symptoms:** Deployment fails during npm install or build
**Solutions:**
1. Check Node.js version compatibility in Render settings
2. Verify package.json dependencies
3. Check build logs for specific error messages
4. Ensure `npm run build` works locally

#### 🔍 Database Connection Fails
**Symptoms:** App starts but can't connect to database
**Solutions:**
1. Verify `DATABASE_URL` environment variable is correct
2. Check database status in Render dashboard
3. Ensure database is in same region as web service
4. Test connection string format

#### 🔍 BigQuery Authentication Fails
**Symptoms:** API endpoints return BigQuery errors
**Solutions:**
1. Verify `GOOGLE_APPLICATION_CREDENTIALS_JSON` is valid JSON
2. Check service account has BigQuery permissions
3. Ensure project ID is correct
4. Test credentials locally first

#### 🔍 Empty Database
**Symptoms:** App works but shows no data
**Solutions:**
1. Run data simulation: `npm run simulate-all:prod`
2. Check database has tables created
3. Verify BigQuery data is accessible
4. Check ETL logs for errors

#### 🔍 Slow Performance
**Symptoms:** App is slow or times out
**Solutions:**
1. Upgrade Render plan (more CPU/memory)
2. Optimize database queries
3. Check database connection pool settings
4. Monitor resource usage in Render dashboard

### Getting Help
- **Render Support**: Available through dashboard chat
- **Database Issues**: Check Render PostgreSQL documentation
- **Application Issues**: Review application logs in Render dashboard

---

## 📈 Phase 10: Monitoring & Maintenance

### Step 10.1: Set Up Monitoring
1. **Render Metrics**: Available in dashboard (CPU, Memory, Response times)
2. **Database Monitoring**: PostgreSQL metrics in database dashboard
3. **Uptime Monitoring**: Consider external service like UptimeRobot

### Step 10.2: Regular Maintenance
**Daily:**
- Check application is responding normally
- Monitor error logs for any issues

**Weekly:**
- Review database performance metrics
- Check disk usage on database
- Verify BigQuery API quotas aren't exceeded

**Monthly:**
- Review and optimize database queries if needed
- Update dependencies if security patches available
- Consider plan upgrades based on usage

---

## 🎯 Success Criteria

Your deployment is successful when:
- [ ] Application loads at your Render URL
- [ ] Dashboard displays 800+ accounts with risk data
- [ ] All API endpoints respond within 2-3 seconds
- [ ] Monthly trends chart shows historical data
- [ ] No errors in Render application logs
- [ ] Database contains populated tables with correct row counts
- [ ] BigQuery integration pulls latest data successfully

---

## 📞 Emergency Rollback Plan

If deployment fails catastrophically:

1. **Rollback Code**:
   ```bash
   git revert <commit-hash>
   git push origin main
   ```
   (Render will auto-deploy the rollback)

2. **Database Recovery**:
   - Render PostgreSQL has automatic backups
   - Can restore from backup in database settings

3. **Quick Recovery**:
   - Deploy from a previous working GitHub commit
   - Use Render's deployment history to redeploy previous version

---

**🎉 Congratulations! You now have ChurnGuard 2.3 running in production on Render with PostgreSQL!**

For ongoing support, refer to the troubleshooting section or reach out with specific error messages and logs.