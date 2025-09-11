# ChurnGuard Build Backup - 2025-09-11T18-56-07

## Files Backed Up
- public/index.js -> index-readable.js
- public/assets/ -> assets/
- public/index.html -> index.html
- src/ -> src/ (source reference)

## Git Status at Backup
```
M scripts/create-monthly-metrics.js
 M scripts/daily-production-etl.js
 M scripts/update-current-month.js
?? build-backups/2025-09-11T18-56-07/
```

## Git Commit
```
008aae3 Complete React environment rebuild and fixes
```

## Purpose
Pre-build backup to preserve readable versions of built files for debugging and emergency edits.

## Restore Instructions
To restore from this backup:
```bash
cp build-backups/2025-09-11T18-56-07/index-readable.js public/index.js
cp -r build-backups/2025-09-11T18-56-07/assets/* public/assets/
cp build-backups/2025-09-11T18-56-07/index.html public/index.html
```
