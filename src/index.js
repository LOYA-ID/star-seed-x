const cron = require('node-cron');
const logger = require('./logger');
const config = require('./config');
const ETLRunner = require('./etl/etlRunner');

/**
 * Main application entry point
 */
class Application {
  constructor() {
    this.etlRunner = null;
    this.cronJob = null;
    this.isShuttingDown = false;
  }

  /**
   * Start the application
   */
  async start() {
    logger.info('========================================');
    logger.info('Star Seed X - ETL Pipeline');
    logger.info('========================================');
    logger.info(`Starting application...`);

    try {
      // Initialize ETL Runner
      this.etlRunner = new ETLRunner();
      await this.etlRunner.initialize();

      // Setup graceful shutdown handlers
      this.setupShutdownHandlers();

      // Run initial ETL job
      logger.info('Running initial ETL job...');
      await this.etlRunner.run();

      // Setup cron schedule
      this.setupCronJob();

      logger.info('Application started successfully');
      logger.info(`Cron schedule: ${config.etl.cronSchedule}`);
      logger.info('Waiting for next scheduled run...');

    } catch (error) {
      logger.error(`Failed to start application: ${error.message}`);
      await this.shutdown(1);
    }
  }

  /**
   * Setup cron job for scheduled ETL runs
   */
  setupCronJob() {
    const cronExpression = config.etl.cronSchedule;

    if (!cron.validate(cronExpression)) {
      logger.error(`Invalid cron expression: ${cronExpression}`);
      throw new Error(`Invalid cron expression: ${cronExpression}`);
    }

    this.cronJob = cron.schedule(cronExpression, async () => {
      if (this.isShuttingDown) {
        logger.info('Shutdown in progress, skipping scheduled ETL run');
        return;
      }

      logger.info('========================================');
      logger.info('Scheduled ETL Job Triggered');
      logger.info('========================================');

      try {
        await this.etlRunner.run();
      } catch (error) {
        logger.error(`Scheduled ETL job failed: ${error.message}`);
      }
    }, {
      scheduled: true,
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone
    });

    logger.info(`Cron job scheduled with expression: ${cronExpression}`);
  }

  /**
   * Setup graceful shutdown handlers
   */
  setupShutdownHandlers() {
    // Handle SIGTERM (PM2, Docker, etc.)
    process.on('SIGTERM', async () => {
      logger.info('Received SIGTERM signal');
      await this.shutdown(0);
    });

    // Handle SIGINT (Ctrl+C)
    process.on('SIGINT', async () => {
      logger.info('Received SIGINT signal');
      await this.shutdown(0);
    });

    // Handle uncaught exceptions
    process.on('uncaughtException', async (error) => {
      logger.error(`Uncaught Exception: ${error.message}`);
      logger.error(error.stack);
      await this.shutdown(1);
    });

    // Handle unhandled promise rejections
    process.on('unhandledRejection', async (reason, promise) => {
      logger.error(`Unhandled Rejection at: ${promise}, reason: ${reason}`);
      await this.shutdown(1);
    });

    logger.debug('Shutdown handlers registered');
  }

  /**
   * Gracefully shutdown the application
   * @param {number} exitCode - Exit code
   */
  async shutdown(exitCode = 0) {
    if (this.isShuttingDown) {
      logger.info('Shutdown already in progress...');
      return;
    }

    this.isShuttingDown = true;
    logger.info('========================================');
    logger.info('Shutting down application...');
    logger.info('========================================');

    try {
      // Stop cron job
      if (this.cronJob) {
        this.cronJob.stop();
        logger.info('Cron job stopped');
      }

      // Wait for current ETL run to complete if running
      if (this.etlRunner && this.etlRunner.isRunning) {
        logger.info('Waiting for current ETL run to complete...');
        // Simple wait mechanism - in production you might want a more sophisticated approach
        let waitCount = 0;
        while (this.etlRunner.isRunning && waitCount < 60) {
          await new Promise(resolve => setTimeout(resolve, 1000));
          waitCount++;
        }
        if (this.etlRunner.isRunning) {
          logger.warn('ETL run did not complete within timeout, forcing shutdown');
        }
      }

      // Shutdown ETL runner
      if (this.etlRunner) {
        await this.etlRunner.shutdown();
      }

      logger.info('Application shut down gracefully');
    } catch (error) {
      logger.error(`Error during shutdown: ${error.message}`);
    } finally {
      process.exit(exitCode);
    }
  }
}

// Create and start application
const app = new Application();
app.start();
