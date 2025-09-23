# ChurnGuard Build Backup - 2025-09-23T22-26-54

## Files Backed Up
- public/index.js -> index-readable.js
- public/assets/ -> assets/
- public/index.html -> index.html
- src/ -> src/ (source reference)

## Git Status at Backup
```
?? build-backups/2025-09-23T22-23-52/
?? build-backups/2025-09-23T22-26-54/
?? scripts/accounts-only.sql
?? scripts/daily-metrics-clean.sql
?? scripts/monthly-metrics-clean.sql
?? scripts/monthly-metrics-only.sql
?? scripts/monthly-metrics-postgres-standard.sql
?? scripts/postgres-data.sql
```

## Git Commit
```
303eb4f Fix ETL to prevent empty risk_reasons arrays
```

## Purpose
Pre-build backup to preserve readable versions of built files for debugging and emergency edits.

## Restore Instructions
To restore from this backup:
```bash
cp build-backups/2025-09-23T22-26-54/index-readable.js public/index.js
cp -r build-backups/2025-09-23T22-26-54/assets/* public/assets/
cp build-backups/2025-09-23T22-26-54/index.html public/index.html
```
