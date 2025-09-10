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

**Last Updated:** September 10, 2025
**Next Review:** Before v2.4 release