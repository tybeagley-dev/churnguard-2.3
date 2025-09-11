#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

const BACKUP_DIR = 'build-backups';
const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
const backupPath = path.join(BACKUP_DIR, timestamp);

console.log('🔧 ChurnGuard Build Backup Script');
console.log(`📅 Timestamp: ${timestamp}`);

async function createBackup() {
  try {
    // Create backup directory structure
    await fs.promises.mkdir(backupPath, { recursive: true });

    // Backup current built files if they exist
    console.log('💾 Backing up current build files...');

    // Backup public directory (contains current built files)
    if (fs.existsSync('public/index.js')) {
      await fs.promises.copyFile('public/index.js', path.join(backupPath, 'index-readable.js'));
      console.log('  ✅ Backed up public/index.js');
    }

    if (fs.existsSync('public/assets')) {
      await fs.promises.cp('public/assets', path.join(backupPath, 'assets'), { recursive: true });
      console.log('  ✅ Backed up public/assets/');
    }

    if (fs.existsSync('public/index.html')) {
      await fs.promises.copyFile('public/index.html', path.join(backupPath, 'index.html'));
      console.log('  ✅ Backed up public/index.html');
    }

    // Backup source files for reference
    if (fs.existsSync('src')) {
      await fs.promises.cp('src', path.join(backupPath, 'src'), { recursive: true });
      console.log('  ✅ Backed up src/ directory');
    }

    // Create a manifest file
    const gitStatus = await execAsync('git status --short 2>/dev/null || echo "Not a git repository"').catch(() => ({ stdout: 'Git not available' }));
    const gitCommit = await execAsync('git log --oneline -1 2>/dev/null || echo "No git history"').catch(() => ({ stdout: 'Git not available' }));

    const manifest = `# ChurnGuard Build Backup - ${timestamp}

## Files Backed Up
- public/index.js -> index-readable.js
- public/assets/ -> assets/
- public/index.html -> index.html
- src/ -> src/ (source reference)

## Git Status at Backup
\`\`\`
${gitStatus.stdout.trim()}
\`\`\`

## Git Commit
\`\`\`
${gitCommit.stdout.trim()}
\`\`\`

## Purpose
Pre-build backup to preserve readable versions of built files for debugging and emergency edits.

## Restore Instructions
To restore from this backup:
\`\`\`bash
cp ${backupPath}/index-readable.js public/index.js
cp -r ${backupPath}/assets/* public/assets/
cp ${backupPath}/index.html public/index.html
\`\`\`
`;

    await fs.promises.writeFile(path.join(backupPath, 'backup-manifest.md'), manifest);
    console.log('📋 Created backup manifest');

    // Keep only last 10 backups (cleanup old ones)
    if (fs.existsSync(BACKUP_DIR)) {
      const backups = await fs.promises.readdir(BACKUP_DIR);
      const sortedBackups = backups
        .filter(dir => fs.statSync(path.join(BACKUP_DIR, dir)).isDirectory())
        .sort((a, b) => b.localeCompare(a)); // Sort newest first

      if (sortedBackups.length > 10) {
        const oldBackups = sortedBackups.slice(10);
        for (const backup of oldBackups) {
          await fs.promises.rm(path.join(BACKUP_DIR, backup), { recursive: true });
        }
        console.log(`🧹 Cleaned up ${oldBackups.length} old backups (keeping 10 most recent)`);
      }
    }

    console.log(`✅ Backup complete: ${backupPath}`);
    console.log('');
    console.log('💡 To restore from backup:');
    console.log(`   cp ${backupPath}/index-readable.js public/index.js`);
    console.log(`   cp -r ${backupPath}/assets/* public/assets/`);

  } catch (error) {
    console.error('❌ Backup failed:', error);
    process.exit(1);
  }
}

createBackup();