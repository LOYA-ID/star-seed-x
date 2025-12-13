/**
 * Load environment variables from .env file (if exists)
 * Must be loaded BEFORE requiring 'config' package
 */
require('dotenv').config();

const config = require('config');
const fs = require('fs');
const path = require('path');

/**
 * Check for mandatory configuration files
 */
const configDir = path.join(process.cwd(), 'config');
const defaultConfigPath = path.join(configDir, 'default.json');

if (!fs.existsSync(defaultConfigPath)) {
  console.error('========================================');
  console.error('ERROR: Missing mandatory configuration file');
  console.error('========================================');
  console.error(`File not found: ${defaultConfigPath}`);
  console.error('');
  console.error('To fix this:');
  console.error('1. Copy config/default.example.json to config/default.json');
  console.error('2. Edit config/default.json with your database settings');
  console.error('');
  console.error('Or use environment variables (see .env.example)');
  console.error('========================================');
  process.exit(1);
}

/**
 * Configuration loader for ETL pipeline
 */
const appConfig = {
  source: {
    host: config.get('source.host'),
    port: config.get('source.port'),
    user: config.get('source.user'),
    password: config.get('source.password'),
    database: config.get('source.database'),
    table: config.get('source.table'),
    connectionPoolSize: config.get('source.connectionPoolSize')
  },
  destination: {
    host: config.get('destination.host'),
    port: config.get('destination.port'),
    user: config.get('destination.user'),
    password: config.get('destination.password'),
    database: config.get('destination.database'),
    table: config.get('destination.table'),
    connectionPoolSize: config.get('destination.connectionPoolSize')
  },
  etl: {
    batchSize: config.get('etl.batchSize'),
    primaryKeyColumn: config.get('etl.primaryKeyColumn'),
    deletedFlagColumn: config.get('etl.deletedFlagColumn'),
    sqlQuery: config.get('etl.sqlQuery'),
    cronSchedule: config.get('etl.cronSchedule')
  },
  sqlite: {
    dbPath: config.get('sqlite.dbPath')
  },
  logging: {
    level: config.get('logging.level'),
    logDir: config.get('logging.logDir')
  }
};

module.exports = appConfig;
