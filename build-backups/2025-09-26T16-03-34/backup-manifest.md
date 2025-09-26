# ChurnGuard Build Backup - 2025-09-26T16-03-34

## Files Backed Up
- public/index.js -> index-readable.js
- public/assets/ -> assets/
- public/index.html -> index.html
- src/ -> src/ (source reference)

## Git Status at Backup
```
M etl/postgresql-experimental/daily-production-etl.js
 M src/components/ui/multi-select.tsx
?? build-backups/2025-09-26T16-03-34/
```

## Git Commit
```
2f8f3d3 Update cron manager to support separate daily and historical monthly rollups
```

## Purpose
Pre-build backup to preserve readable versions of built files for debugging and emergency edits.

## Restore Instructions
To restore from this backup:
```bash
cp build-backups/2025-09-26T16-03-34/index-readable.js public/index.js
cp -r build-backups/2025-09-26T16-03-34/assets/* public/assets/
cp build-backups/2025-09-26T16-03-34/index.html public/index.html
```
