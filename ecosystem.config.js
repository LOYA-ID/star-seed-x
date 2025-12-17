const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

/**
 * Auto-discover job configuration files from config/ directory
 * Only reads files ending with .json (excludes .json.example, etc.)
 */
function discoverJobs() {
  const configDir = path.join(__dirname, 'config');
  
  if (!fs.existsSync(configDir)) {
    console.warn('Warning: config directory not found');
    return [];
  }
  
  const files = fs.readdirSync(configDir);
  
  // Only include files ending with .json (excludes .json.example)
  const jobFiles = files.filter(file => file.endsWith('.json'));
  
  return jobFiles.map(file => file.replace('.json', ''));
}

/**
 * Delete existing PM2 processes for discovered jobs to avoid duplicates
 */
function cleanupExistingProcesses(jobs) {
  jobs.forEach(jobName => {
    const processName = `star-seed-x-${jobName}`;
    try {
      // Try to delete existing process (ignore errors if not exists)
      execSync(`pm2 delete ${processName}`, { stdio: 'ignore' });
      console.log(`Deleted existing PM2 process: ${processName}`);
    } catch (e) {
      // Process doesn't exist, that's fine
    }
  });
}

/**
 * Generate PM2 app configuration for a job
 */
function createAppConfig(jobName) {
  return {
    name: `star-seed-x-${jobName}`,
    script: 'src/index.js',
    args: `--job ${jobName}`,
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '500M',
    env: {
      NODE_ENV: 'development',
      ETL_JOB_NAME: jobName
    },
    env_production: {
      NODE_ENV: 'production',
      ETL_JOB_NAME: jobName
    },
    error_file: `./logs/${jobName}/pm2-error.log`,
    out_file: `./logs/${jobName}/pm2-out.log`,
    log_file: `./logs/${jobName}/pm2-combined.log`,
    time: true,
    // Graceful shutdown configuration
    kill_timeout: 30000,
    wait_ready: true,
    listen_timeout: 10000,
    // Restart configuration
    exp_backoff_restart_delay: 1000,
    max_restarts: 10,
    restart_delay: 1000
  };
}

// Discover all jobs
const jobs = discoverJobs();

// Cleanup existing processes before starting
cleanupExistingProcesses(jobs);

// Generate PM2 configuration
const apps = jobs.map(jobName => createAppConfig(jobName));

if (apps.length === 0) {
  console.warn('========================================');
  console.warn('Warning: No job configuration files found');
  console.warn('========================================');
  console.warn('Create job config files in the config/ directory:');
  console.warn('  cp config/job.json.example config/myjob.json');
  console.warn('Then edit config/myjob.json with your settings');
  console.warn('========================================');
}

console.log(`Discovered ${apps.length} job(s): ${jobs.join(', ') || 'none'}`);

module.exports = {
  apps
};
