# ChurnGuard Build Backup - 2025-09-29T20-21-30

## Files Backed Up
- public/index.js -> index-readable.js
- public/assets/ -> assets/
- public/index.html -> index.html
- src/ -> src/ (source reference)

## Git Status at Backup
```
M src/components/dashboard/account-metrics-table-monthly.tsx
?? build-backups/2025-09-29T20-21-30/
```

## Git Commit
```
4a0d06b Add selective write protection for manually corrected account data
```

## Purpose
Pre-build backup to preserve readable versions of built files for debugging and emergency edits.

## Restore Instructions
To restore from this backup:
```bash
cp build-backups/2025-09-29T20-21-30/index-readable.js public/index.js
cp -r build-backups/2025-09-29T20-21-30/assets/* public/assets/
cp build-backups/2025-09-29T20-21-30/index.html public/index.html
```
