# ChurnGuard Build Backup - 2025-09-17T14-51-02

## Files Backed Up
- public/index.js -> index-readable.js
- public/assets/ -> assets/
- public/index.html -> index.html
- src/ -> src/ (source reference)

## Git Status at Backup
```
M package.json
 M server-clean.js
 M src/components/dashboard/claude-12month-chart.tsx
 M vite.config.ts
?? DEPLOYMENT.md
?? build-backups/2025-09-17T14-51-02/
?? src/controllers/accounts.controller.js
?? src/controllers/auth.controller.js
?? src/routes/accounts.routes.js
?? src/routes/auth.routes.js
?? src/services/accounts.service.js
```

## Git Commit
```
eef5efa Complete Monthly Trends and Weekly View clean architecture implementation
```

## Purpose
Pre-build backup to preserve readable versions of built files for debugging and emergency edits.

## Restore Instructions
To restore from this backup:
```bash
cp build-backups/2025-09-17T14-51-02/index-readable.js public/index.js
cp -r build-backups/2025-09-17T14-51-02/assets/* public/assets/
cp build-backups/2025-09-17T14-51-02/index.html public/index.html
```
