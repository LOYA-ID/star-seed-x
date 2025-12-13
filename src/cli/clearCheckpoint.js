#!/usr/bin/env node

/**
 * CLI command to clear ETL checkpoints and state
 * Usage: node src/cli/clearCheckpoint.js [options]
 * 
 * Options:
 *   --all       Clear all checkpoints globally (all source/dest pairs)
 *   --help      Show help message
 */

// Load environment variables first
require('dotenv').config();

const sqliteManager = require('../database/sqlite');
const config = require('../config');

// Parse command line arguments
const args = process.argv.slice(2);
const showHelp = args.includes('--help') || args.includes('-h');
const clearAll = args.includes('--all') || args.includes('-a');

if (showHelp) {
  console.log(`
Star Seed X - Clear Checkpoint Utility

Usage: 
  npm run etl:clear-checkpoint         Clear checkpoints for configured tables
  npm run etl:clear-checkpoint -- --all  Clear ALL checkpoints globally

Options:
  --all, -a    Clear all checkpoints globally (all source/dest pairs)
  --help, -h   Show this help message

Description:
  This utility clears ETL checkpoints stored in SQLite, allowing you to 
  restart the ETL process from scratch.

  Without --all flag:
    Clears checkpoints only for the source/destination tables 
    configured in config/default.json

  With --all flag:
    Clears ALL checkpoints in the database (use with caution!)

Examples:
  npm run etl:clear-checkpoint
  npm run etl:clear-checkpoint -- --all
`);
  process.exit(0);
}

// Initialize SQLite
console.log('========================================');
console.log('Star Seed X - Clear Checkpoint');
console.log('========================================');

try {
  sqliteManager.initialize();

  if (clearAll) {
    console.log('\nClearing ALL checkpoints globally...');
    const count = sqliteManager.clearAllCheckpointsGlobal();
    console.log(`✅ Cleared ${count} checkpoint(s) globally`);
  } else {
    const sourceTable = config.source.table;
    const destTable = config.destination.table;
    
    console.log(`\nSource table: ${sourceTable}`);
    console.log(`Destination table: ${destTable}`);
    console.log('\nClearing checkpoints and incremental state...');
    
    const result = sqliteManager.clearAllState(sourceTable, destTable);
    
    console.log(`✅ Cleared ${result.checkpoints} checkpoint(s)`);
    console.log(`✅ Cleared ${result.incrementalState} incremental state record(s)`);
  }

  console.log('\n✅ Done! ETL will start fresh on next run.');
  console.log('========================================');

} catch (error) {
  console.error(`\n❌ Error: ${error.message}`);
  process.exit(1);
} finally {
  sqliteManager.close();
}
