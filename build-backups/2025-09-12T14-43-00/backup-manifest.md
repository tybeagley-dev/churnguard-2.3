# ChurnGuard Build Backup - 2025-09-12T14-43-00

## Files Backed Up
- public/index.js -> index-readable.js
- public/assets/ -> assets/
- public/index.html -> index.html
- src/ -> src/ (source reference)

## Git Status at Backup
```
?? build-backups/2025-09-12T14-43-00/
```

## Git Commit
```
35d45e3 Fix current month risk levels with trending logic
```

## Purpose
Pre-build backup to preserve readable versions of built files for debugging and emergency edits.

## Restore Instructions
To restore from this backup:
```bash
cp build-backups/2025-09-12T14-43-00/index-readable.js public/index.js
cp -r build-backups/2025-09-12T14-43-00/assets/* public/assets/
cp build-backups/2025-09-12T14-43-00/index.html public/index.html
```
