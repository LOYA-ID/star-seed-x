#!/usr/bin/env node

/**
 * Kill existing ETL processes before starting new ones
 * Works with both npm start (node processes) and PM2
 */

const { execSync, spawnSync } = require('child_process');
const path = require('path');

// Get job name from command line arguments
const args = process.argv.slice(2);
let jobName = null;

// Parse arguments (support: jobname, --job jobname, --job=jobname)
for (let i = 0; i < args.length; i++) {
  const arg = args[i];
  if (arg === '--job' && args[i + 1]) {
    jobName = args[i + 1];
    break;
  } else if (arg.startsWith('--job=')) {
    jobName = arg.split('=')[1];
    break;
  } else if (!arg.startsWith('-')) {
    jobName = arg;
    break;
  }
}

const isWindows = process.platform === 'win32';

/**
 * Kill PM2 process by name
 */
function killPM2Process(processName) {
  try {
    execSync(`pm2 delete ${processName}`, { stdio: 'ignore' });
    console.log(`✓ Killed PM2 process: ${processName}`);
    return true;
  } catch (e) {
    return false;
  }
}

/**
 * Kill Node processes running src/index.js with specific job
 */
function killNodeProcesses(job) {
  if (isWindows) {
    return killNodeProcessesWindows(job);
  } else {
    return killNodeProcessesUnix(job);
  }
}

/**
 * Kill Node processes on Windows
 */
function killNodeProcessesWindows(job) {
  try {
    // Find node processes running index.js
    const result = spawnSync('wmic', [
      'process', 'where', 
      "name='node.exe' and commandline like '%src/index.js%'",
      'get', 'processid,commandline', '/format:csv'
    ], { encoding: 'utf8', shell: true });
    
    if (result.stdout) {
      const lines = result.stdout.split('\n').filter(line => line.trim());
      let killed = 0;
      
      for (const line of lines) {
        // Check if this line contains our job
        if (job && !line.includes(job)) continue;
        
        // Extract PID
        const match = line.match(/(\d+)$/);
        if (match) {
          const pid = match[1];
          try {
            execSync(`taskkill /F /PID ${pid}`, { stdio: 'ignore' });
            console.log(`✓ Killed Node process PID: ${pid}`);
            killed++;
          } catch (e) {
            // Process already gone
          }
        }
      }
      return killed > 0;
    }
  } catch (e) {
    // WMIC not available or error
  }
  return false;
}

/**
 * Kill Node processes on Unix/Linux/Mac
 */
function killNodeProcessesUnix(job) {
  try {
    // Find node processes running index.js
    const pattern = job 
      ? `node.*src/index.js.*${job}`
      : 'node.*src/index.js';
    
    const result = spawnSync('pgrep', ['-f', pattern], { encoding: 'utf8' });
    
    if (result.stdout) {
      const pids = result.stdout.trim().split('\n').filter(pid => pid && pid !== process.pid.toString());
      
      for (const pid of pids) {
        try {
          process.kill(parseInt(pid), 'SIGTERM');
          console.log(`✓ Killed Node process PID: ${pid}`);
        } catch (e) {
          // Process already gone
        }
      }
      return pids.length > 0;
    }
  } catch (e) {
    // pgrep not available or no processes found
  }
  return false;
}

/**
 * Main cleanup function
 */
function cleanup() {
  console.log('========================================');
  console.log('Cleaning up existing processes...');
  console.log('========================================');
  
  let cleanedAny = false;
  
  if (jobName) {
    // Kill specific job
    console.log(`Target job: ${jobName}`);
    
    // Kill PM2 process
    if (killPM2Process(`star-seed-x-${jobName}`)) {
      cleanedAny = true;
    }
    
    // Kill Node processes
    if (killNodeProcesses(jobName)) {
      cleanedAny = true;
    }
  } else {
    // Kill all star-seed-x jobs
    console.log('Target: all star-seed-x jobs');
    
    // Try to get list of jobs from config directory
    const configDir = path.join(__dirname, '..', '..', 'config');
    const fs = require('fs');
    
    if (fs.existsSync(configDir)) {
      const files = fs.readdirSync(configDir);
      const jobs = files
        .filter(file => file.endsWith('.json'))
        .map(file => file.replace('.json', ''));
      
      for (const job of jobs) {
        if (killPM2Process(`star-seed-x-${job}`)) {
          cleanedAny = true;
        }
      }
    }
    
    // Kill all Node processes running index.js
    if (killNodeProcesses(null)) {
      cleanedAny = true;
    }
  }
  
  if (!cleanedAny) {
    console.log('No existing processes found');
  }
  
  console.log('========================================');
  console.log('Cleanup complete');
  console.log('========================================');
}

// Run cleanup
cleanup();
