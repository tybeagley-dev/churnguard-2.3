# ChurnGuard Build Backup - 2025-09-11T15-50-44

## Files Backed Up
- public/index.js -> index-readable.js
- public/assets/ -> assets/
- public/index.html -> index.html
- src/ -> src/ (source reference)

## Git Status at Backup
```
M package-lock.json
 M package.json
 M public/index.html
 M server.js
?? build-backups/
?? index.html
?? postcss.config.js
?? scripts/backup-build.js
?? src/
?? tailwind.config.ts
?? tsconfig.json
?? vite.config.ts
```

## Git Commit
```
c50357b Remove sensitive files and improve .gitignore security
```

## Purpose
Pre-build backup to preserve readable versions of built files for debugging and emergency edits.

## Restore Instructions
To restore from this backup:
```bash
cp build-backups/2025-09-11T15-50-44/index-readable.js public/index.js
cp -r build-backups/2025-09-11T15-50-44/assets/* public/assets/
cp build-backups/2025-09-11T15-50-44/index.html public/index.html
```
