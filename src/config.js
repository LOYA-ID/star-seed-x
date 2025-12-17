/**
 * Configuration loader for multi-job ETL pipeline
 * Supports job-specific configuration files in config/{jobName}.json
 */
const { currentJobName, loadJobConfig, listAvailableJobs } = require('./jobConfig');

// Validate that a job name is provided
if (!currentJobName) {
  console.error('========================================');
  console.error('ERROR: No job name specified');
  console.error('========================================');
  console.error('Usage: npm run start -- <jobname>');
  console.error('       npm run start -- --job <jobname>');
  console.error('       node src/index.js <jobname>');
  console.error('');
  
  const availableJobs = listAvailableJobs();
  if (availableJobs.length > 0) {
    console.error('Available jobs:');
    availableJobs.forEach(job => console.error(`  - ${job}`));
  } else {
    console.error('No job configuration files found.');
    console.error('Create a job config file:');
    console.error('  cp config/job.json.example config/<jobname>.json');
  }
  
  console.error('========================================');
  process.exit(1);
}

// Load job-specific configuration
const jobConfig = loadJobConfig(currentJobName);

/**
 * Configuration object for ETL pipeline
 */
const appConfig = {
  jobName: currentJobName,
  source: {
    host: jobConfig.source.host,
    port: jobConfig.source.port,
    user: jobConfig.source.user,
    password: jobConfig.source.password,
    database: jobConfig.source.database,
    table: jobConfig.source.table,
    connectionPoolSize: jobConfig.source.connectionPoolSize || 5,
    queryTimeout: jobConfig.source.queryTimeout || 300000,
    maxRetries: jobConfig.source.maxRetries || 3,
    retryDelay: jobConfig.source.retryDelay || 1000
  },
  destination: {
    host: jobConfig.destination.host,
    port: jobConfig.destination.port,
    user: jobConfig.destination.user,
    password: jobConfig.destination.password,
    database: jobConfig.destination.database,
    table: jobConfig.destination.table,
    connectionPoolSize: jobConfig.destination.connectionPoolSize || 5,
    queryTimeout: jobConfig.destination.queryTimeout || 300000,
    maxRetries: jobConfig.destination.maxRetries || 3,
    retryDelay: jobConfig.destination.retryDelay || 1000
  },
  etl: {
    batchSize: jobConfig.etl.batchSize || 1000,
    primaryKeyColumn: jobConfig.etl.primaryKeyColumn || 'id',
    deletedFlagColumn: jobConfig.etl.deletedFlagColumn || 'is_deleted',
    sqlQuery: jobConfig.etl.sqlQuery || 'SELECT * FROM {{table}}',
    cronSchedule: jobConfig.etl.cronSchedule || '*/5 * * * *',
    maxRetries: jobConfig.etl.maxRetries || 3,
    retryDelay: jobConfig.etl.retryDelay || 1000,
    forceFullRefresh: jobConfig.etl.forceFullRefresh || false,
    recordDelay: jobConfig.etl.recordDelay || 0
  },
  sqlite: {
    dbPath: jobConfig.sqlite.dbPath
  },
  logging: {
    level: jobConfig.logging.level,
    logDir: jobConfig.logging.logDir
  }
};

module.exports = appConfig;
