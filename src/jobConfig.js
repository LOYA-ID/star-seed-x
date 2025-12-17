/**
 * Job configuration loader for multi-job ETL pipeline
 * Loads job-specific config from config/{jobName}.json
 */
const fs = require('fs');
const path = require('path');

/**
 * Parse command line arguments to get job name
 * Supports multiple formats:
 *   - Positional: node src/index.js jobname
 *   - Flag: node src/index.js --job jobname
 *   - Flag with equals: node src/index.js --job=jobname
 * @returns {string|null} Job name or null if not specified
 */
function getJobNameFromArgs() {
  const args = process.argv.slice(2);
  
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    
    // Handle --job=jobname format
    if (arg.startsWith('--job=')) {
      return arg.split('=')[1];
    }
    
    // Handle --job jobname format
    if (arg === '--job' && args[i + 1]) {
      return args[i + 1];
    }
  }
  
  // Handle positional argument (first non-flag argument)
  // Skip arguments that start with - or are values for flags
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    
    // Skip flags and their values
    if (arg.startsWith('-')) {
      // If it's a flag that expects a value (like --job), skip next arg too
      if (arg === '--job' || arg === '-j') {
        i++;
      }
      continue;
    }
    
    // Found a positional argument - use it as job name
    return arg;
  }
  
  return null;
}

/**
 * Get job name from environment variable or command line
 * @returns {string|null} Job name
 */
function getJobName() {
  // First check environment variable (set by PM2)
  if (process.env.ETL_JOB_NAME) {
    return process.env.ETL_JOB_NAME;
  }
  
  // Then check command line arguments
  return getJobNameFromArgs();
}

/**
 * Load job configuration from file
 * @param {string} jobName - Name of the job (without .json extension)
 * @returns {object} Configuration object
 */
function loadJobConfig(jobName) {
  // Use __dirname to get config directory relative to this file
  const configDir = path.join(__dirname, '..', 'config');
  const jobConfigPath = path.join(configDir, `${jobName}.json`);
  
  if (!fs.existsSync(jobConfigPath)) {
    console.error('========================================');
    console.error('ERROR: Job configuration file not found');
    console.error('========================================');
    console.error(`File not found: ${jobConfigPath}`);
    console.error('');
    console.error('To fix this:');
    console.error(`1. Copy config/job.json.example to config/${jobName}.json`);
    console.error('2. Edit the file with your database settings');
    console.error('========================================');
    process.exit(1);
  }
  
  try {
    const configContent = fs.readFileSync(jobConfigPath, 'utf8');
    const config = JSON.parse(configContent);
    
    // Add job name to config
    config.jobName = jobName;
    
    // Set job-specific paths for sqlite and logs
    if (!config.sqlite) {
      config.sqlite = {};
    }
    if (!config.sqlite.dbPath) {
      config.sqlite.dbPath = `./data/${jobName}_state.db`;
    }
    
    if (!config.logging) {
      config.logging = {};
    }
    if (!config.logging.logDir) {
      config.logging.logDir = `./logs/${jobName}`;
    }
    if (!config.logging.level) {
      config.logging.level = 'info';
    }
    
    return config;
  } catch (error) {
    console.error('========================================');
    console.error('ERROR: Failed to parse job configuration');
    console.error('========================================');
    console.error(`File: ${jobConfigPath}`);
    console.error(`Error: ${error.message}`);
    console.error('========================================');
    process.exit(1);
  }
}

/**
 * List all available job configuration files
 * @returns {string[]} Array of job names (without .json extension)
 */
function listAvailableJobs() {
  // Use __dirname to get config directory relative to this file
  const configDir = path.join(__dirname, '..', 'config');
  
  if (!fs.existsSync(configDir)) {
    return [];
  }
  
  const files = fs.readdirSync(configDir);
  
  // Only include files ending with .json (excludes .json.example, etc.)
  return files
    .filter(file => file.endsWith('.json'))
    .map(file => file.replace('.json', ''));
}

// Get job name
const jobName = getJobName();

// Export job utilities
module.exports = {
  getJobName,
  getJobNameFromArgs,
  loadJobConfig,
  listAvailableJobs,
  currentJobName: jobName
};
