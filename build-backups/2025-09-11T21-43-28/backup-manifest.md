# ChurnGuard Build Backup - 2025-09-11T21-43-28

## Files Backed Up
- public/index.js -> index-readable.js
- public/assets/ -> assets/
- public/index.html -> index.html
- src/ -> src/ (source reference)

## Git Status at Backup
```
M server.js
 M src/components/dashboard/account-metrics-table.tsx
?? build-backups/2025-09-11T21-43-28/
```

## Git Commit
```
9b0806c Complete ChurnGuard 2.3 Monthly View unified approach implementation
```

## Purpose
Pre-build backup to preserve readable versions of built files for debugging and emergency edits.

## Restore Instructions
To restore from this backup:
```bash
cp build-backups/2025-09-11T21-43-28/index-readable.js public/index.js
cp -r build-backups/2025-09-11T21-43-28/assets/* public/assets/
cp build-backups/2025-09-11T21-43-28/index.html public/index.html
```
