# ChurnGuard Database Migration Plan

## Month Label Technical Debt Cleanup

### Current Issue
The `month_label` field is currently hardcoded in SQL with static CASE statements that only work for 2025:

```sql
CASE month 
  WHEN '2025-07' THEN 'July 2025'
  WHEN '2025-08' THEN 'August 2025'
  -- etc... BREAKS IN 2026
END as month_label
```

This creates maintenance debt and will break in January 2026.

### Migration Strategy

#### Phase 1: Fix Hardcoding (Current - v2.3)
**Status:** In Progress
- ✅ Keep `month_label` column in database for compatibility
- ✅ Replace hardcoded SQL CASE statements with dynamic JavaScript generation
- ✅ Maintain full API compatibility with frontend

**Implementation:**
```javascript
function formatMonthLabel(monthString) {
  const [year, month] = monthString.split('-');
  const monthNames = ['January', 'February', 'March', 'April', 'May', 'June',
                     'July', 'August', 'September', 'October', 'November', 'December'];
  const monthIndex = parseInt(month) - 1;
  return `${monthNames[monthIndex]} ${year}`;
}
```

#### Phase 2: Frontend Formatting (Future - v2.4+)
**Status:** Planned
- [ ] Update frontend to format month labels client-side
- [ ] Remove API dependency on `month_label` field
- [ ] API returns only raw `month` field (YYYY-MM format)
- [ ] Frontend handles all date formatting and localization

**Benefits of Phase 2:**
- Reduced database storage
- Better separation of concerns (data vs presentation)
- Easier internationalization support
- Cleaner API responses

### Current Frontend Dependencies
These API endpoints currently return `month_label`:
- `/api/monthly-trends` - uses `mm.month_label as periodLabel`
- `/api/risk-distribution-trends` - maps `month_label` to `periodLabel`, `monthLabel`
- Various dashboard endpoints expect `month_label` in response

### Files to Update in Phase 2
- `server.js` - Remove month_label from all queries
- `public/index.js` - Add client-side month formatting
- Database schema - Remove month_label column
- All ETL scripts - Remove month_label generation

### Testing Checklist
- [ ] Monthly trends chart displays correct labels
- [ ] Risk distribution trends show proper month names
- [ ] Dashboard summaries format months correctly
- [ ] Year transitions work properly (Dec 2025 → Jan 2026)

---

## SQLite to PostgreSQL Database Migration

### Current Issue
ChurnGuard 2.3 currently uses SQLite for simulation and development, but production deployment requires PostgreSQL. Several SQLite-specific functions and patterns need conversion.

### Migration Assessment: MODERATE COMPLEXITY (6/10)

#### **Critical SQLite-Specific Code Found**

**Location:** `/etl/daily-production-etl.js:299-313`
```sql
-- SQLite Functions → PostgreSQL Equivalents
datetime('now') → NOW() or CURRENT_TIMESTAMP
strftime('%Y-%m', 'now') → TO_CHAR(CURRENT_DATE, 'YYYY-MM')
strftime('%Y-%m', a.launched_at) → TO_CHAR(a.launched_at, 'YYYY-MM')
INSERT OR REPLACE → INSERT ... ON CONFLICT ... DO UPDATE
```

#### **Data Type Mapping Requirements**

| SQLite Type | PostgreSQL Type | Notes |
|-------------|-----------------|-------|
| `TEXT` | `VARCHAR(255)` or `TEXT` | Primary keys should use VARCHAR(255) |
| `INTEGER` | `INTEGER` or `BIGINT` | Depends on expected range |
| `REAL` | `DECIMAL(10,2)` or `NUMERIC` | For monetary values |
| `TEXT` (dates) | `TIMESTAMP` or `DATE` | Based on precision needed |

#### **Connection Architecture Changes**

**Current SQLite Pattern:**
```javascript
// config/database.js
const db = await open({
  filename: this.dbPath,
  driver: sqlite3.Database
});
```

**Required PostgreSQL Pattern:**
```javascript
// Already implemented in run-full-simulation.js
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});
```

#### **Transaction Handling Updates**

**SQLite:**
```javascript
await db.exec('BEGIN TRANSACTION');
// operations
await db.exec('COMMIT');
```

**PostgreSQL:**
```javascript
const client = await pool.connect();
await client.query('BEGIN');
// operations
await client.query('COMMIT');
client.release();
```

### Migration Strategy

#### Phase 1: Function Replacement (High Priority)
**Status:** Ready to Execute
- [ ] Replace `datetime('now')` with `NOW()`
- [ ] Replace `strftime()` calls with `TO_CHAR()`
- [ ] Convert `INSERT OR REPLACE` to `INSERT ... ON CONFLICT`
- [ ] Update date comparison logic

#### Phase 2: Connection Layer Switch (High Priority)
**Status:** Infrastructure Ready
- [ ] Switch `config/database.js` from sqlite3 to pg Pool
- [ ] Update all `*-sqlite.js` ETL files to use PostgreSQL versions
- [ ] Verify connection pooling and SSL configuration

#### Phase 3: Schema Validation (Medium Priority)
**Status:** Planned
- [ ] Validate data type mappings in production
- [ ] Test index performance on PostgreSQL
- [ ] Verify foreign key constraints work correctly

#### Phase 4: Testing & Validation (High Priority)
**Status:** Required Before Production
- [ ] Test all ETL pipelines against PostgreSQL
- [ ] Validate data integrity across migration
- [ ] Performance test with production data volumes
- [ ] Test connection pooling under load

### Files Requiring Updates

**High Priority:**
1. `etl/daily-production-etl.js` - Contains SQLite-specific functions
2. `config/database.js` - Switch from sqlite3 to pg Pool
3. All `*-sqlite.js` ETL files - Replace with PostgreSQL versions

**Medium Priority:**
4. Server startup scripts - Database initialization
5. Environment configuration - Ensure DATABASE_URL is configured

### Existing PostgreSQL Infrastructure

✅ **Already Implemented:**
- PostgreSQL connection pooling in `/etl/run-full-simulation.js`
- Environment variable `DATABASE_URL` configured in `.env.example`
- SSL support for production deployment
- PostgreSQL ETL classes exist (`/etl/accounts-etl.js`)

### Risk Mitigation

**Low Risk Factors:**
- Infrastructure already exists
- No complex schema changes required
- ETL patterns translate directly

**Medium Risk Factors:**
- Date/time function replacements throughout codebase
- INSERT OR REPLACE logic conversion requires careful testing
- Transaction pattern updates across multiple files

### Estimated Timeline
- **Core Function Updates:** 4-6 hours
- **Connection Layer Migration:** 2-3 hours
- **Testing & Validation:** 4-6 hours
- **Total Migration Time:** 10-15 hours

### Testing Checklist
- [ ] All ETL pipelines run successfully on PostgreSQL
- [ ] Date/time calculations produce identical results
- [ ] INSERT OR REPLACE logic maintains data integrity
- [ ] Connection pooling performs adequately under load
- [ ] All existing API endpoints return identical data
- [ ] Dashboard functionality unchanged

---

**Last Updated:** September 19, 2025
**Next Review:** Before PostgreSQL production deployment