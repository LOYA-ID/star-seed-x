#!/usr/bin/env node

/**
 * CLI tool to list available ETL jobs
 */
const { listAvailableJobs } = require('../jobConfig');

console.log('========================================');
console.log('Star Seed X - Available ETL Jobs');
console.log('========================================');

const jobs = listAvailableJobs();

if (jobs.length === 0) {
  console.log('No job configuration files found.');
  console.log('');
  console.log('To create a job:');
  console.log('  cp config/job.json.example config/myjob.json');
  console.log('');
  console.log('Then edit config/myjob.json with your database settings.');
  console.log('');
  console.log('Only files ending with .json are detected as jobs.');
} else {
  console.log(`Found ${jobs.length} job(s):`);
  console.log('');
  jobs.forEach(job => {
    console.log(`  - ${job}`);
    console.log(`    Config: config/${job}.json`);
    console.log(`    Run:    npm run start -- ${job}`);
    console.log(`    PM2:    pm2 start src/index.js --name star-seed-x-${job} -- ${job}`);
    console.log('');
  });
}

console.log('========================================');
console.log('Commands:');
console.log('  Single job:      npm run start -- <jobname>');
console.log('  All jobs (PM2):  npm run pm2:start');
console.log('  Kill job:        npm run kill -- <jobname>');
console.log('  Kill all:        npm run kill:all');
console.log('  List status:     npm run pm2:list');
console.log('========================================');
