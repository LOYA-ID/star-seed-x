#!/usr/bin/env node

/**
 * CLI command to clear ETL checkpoints and state
 * Usage: node src/cli/clearCheckpoint.js --job <jobname> [options]
 * 
 * Options:
 *   --job       Job name (required)
 *   --all       Clear all checkpoints globally (all source/dest pairs)
 *   --help      Show help message
 */

const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const { loadJobConfig, listAvailableJobs, getJobNameFromArgs } = require('../jobConfig');

// Parse command line arguments
const args = process.argv.slice(2);
const showHelp = args.includes('--help') || args.includes('-h');
const clearAll = args.includes('--all') || args.includes('-a');

// Get job name from arguments
const jobName = getJobNameFromArgs();

if (showHelp) {
  console.log(`
Star Seed X - Clear Checkpoint Utility

Usage: 
  npm run etl:clear-checkpoint -- --job <jobname>         Clear checkpoints for job
  npm run etl:clear-checkpoint -- --job <jobname> --all   Clear ALL checkpoints for job

Options:
  --job <name>  Job name (required) - name of the config file without .json
  --all, -a     Clear all checkpoints globally (all source/dest pairs)
  --help, -h    Show this help message

Description:
  This utility clears ETL checkpoints stored in SQLite, allowing you to 
  restart the ETL process from scratch.

  Without --all flag:
    Clears checkpoints only for the source/destination tables 
    configured in the job config file

  With --all flag:
    Clears ALL checkpoints in the job's database (use with caution!)

Examples:
  npm run etl:clear-checkpoint -- --job job1
  npm run etl:clear-checkpoint -- --job job1 --all
`);
  
  const jobs = listAvailableJobs();
  if (jobs.length > 0) {
    console.log('Available jobs:');
    jobs.forEach(job => console.log(`  - ${job}`));
  }
  
  process.exit(0);
}

// Validate job name
if (!jobName) {
  console.error('========================================');
  console.error('ERROR: No job name specified');
  console.error('========================================');
  console.error('Usage: npm run etl:clear-checkpoint -- --job <jobname>');
  console.error('');
  
  const jobs = listAvailableJobs();
  if (jobs.length > 0) {
    console.error('Available jobs:');
    jobs.forEach(job => console.error(`  - ${job}`));
  }
  
  process.exit(1);
}

// Load job configuration
const config = loadJobConfig(jobName);

// Initialize SQLite for this job
console.log('========================================');
console.log('Star Seed X - Clear Checkpoint');
console.log(`Job: ${jobName}`);
console.log('========================================');

try {
  // Ensure data directory exists
  const dbDir = path.dirname(config.sqlite.dbPath);
  if (!fs.existsSync(dbDir)) {
    fs.mkdirSync(dbDir, { recursive: true });
  }
  
  const db = new Database(config.sqlite.dbPath);
  
  // Create tables if they don't exist
  db.exec(`
    CREATE TABLE IF NOT EXISTS incremental_state (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_table TEXT NOT NULL,
      destination_table TEXT NOT NULL,
      primary_key_column TEXT NOT NULL,
      last_processed_value TEXT,
      last_run_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(source_table, destination_table, primary_key_column)
    )
  `);
  
  db.exec(`
    CREATE TABLE IF NOT EXISTS etl_checkpoints (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_table TEXT NOT NULL,
      destination_table TEXT NOT NULL,
      checkpoint_type TEXT NOT NULL,
      checkpoint_value TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(source_table, destination_table, checkpoint_type)
    )
  `);

  if (clearAll) {
    console.log('\nClearing ALL checkpoints globally...');
    const result1 = db.prepare('DELETE FROM etl_checkpoints').run();
    const result2 = db.prepare('DELETE FROM incremental_state').run();
    console.log(`[OK] Cleared ${result1.changes} checkpoint(s)`);
    console.log(`[OK] Cleared ${result2.changes} incremental state record(s)`);
  } else {
    const sourceTable = config.source.table;
    const destTable = config.destination.table;
    
    console.log(`\nSource table: ${sourceTable}`);
    console.log(`Destination table: ${destTable}`);
    console.log('\nClearing checkpoints and incremental state...');
    
    const result1 = db.prepare(
      'DELETE FROM etl_checkpoints WHERE source_table = ? AND destination_table = ?'
    ).run(sourceTable, destTable);
    
    const result2 = db.prepare(
      'DELETE FROM incremental_state WHERE source_table = ? AND destination_table = ?'
    ).run(sourceTable, destTable);
    
    console.log(`[OK] Cleared ${result1.changes} checkpoint(s)`);
    console.log(`[OK] Cleared ${result2.changes} incremental state record(s)`);
  }

  db.close();
  console.log('\n[OK] Done! ETL will start fresh on next run.');
  console.log('========================================');

} catch (error) {
  console.error(`\n[ERROR] ${error.message}`);
  process.exit(1);
}
