const logger = require('../logger');
const config = require('../config');
const MariaDBPool = require('../database/mariadb');
const sqliteManager = require('../database/sqlite');
const ConnectionChecker = require('../utils/connectionChecker');
const SchemaValidator = require('../utils/schemaValidator');
const ModeDetector = require('./modeDetector');
const FullLoadProcessor = require('./fullLoad');
const IncrementalLoadProcessor = require('./incrementalLoad');
const DeltaLoadProcessor = require('./deltaLoad');

/**
 * Main ETL runner class
 */
class ETLRunner {
  constructor() {
    this.sourcePool = null;
    this.destPool = null;
    this.isRunning = false;
  }

  /**
   * Initialize database connections
   */
  async initialize() {
    logger.info('Initializing ETL Runner...');

    try {
      // Initialize source MariaDB pool
      this.sourcePool = new MariaDBPool('Source', config.source);
      await this.sourcePool.initialize();

      // Initialize destination MariaDB pool
      this.destPool = new MariaDBPool('Destination', config.destination);
      await this.destPool.initialize();

      // Initialize SQLite for state management
      sqliteManager.initialize();

      logger.info('ETL Runner initialized successfully');
      return true;
    } catch (error) {
      logger.error(`Failed to initialize ETL Runner: ${error.message}`);
      throw error;
    }
  }

  /**
   * Run the ETL process
   * @param {string} forceMode - Optional mode to force ('full', 'incremental', 'delta')
   */
  async run(forceMode = null) {
    if (this.isRunning) {
      logger.warn('ETL process is already running, skipping this execution');
      return;
    }

    this.isRunning = true;
    const startTime = new Date();
    let runResult = {
      sourceTable: config.source.table,
      destinationTable: config.destination.table,
      mode: null,
      rowsProcessed: 0,
      rowsInserted: 0,
      rowsDeleted: 0,
      status: 'started',
      errorMessage: null,
      startTime: startTime.toISOString(),
      endTime: null,
      durationSeconds: null
    };

    try {
      logger.info('========================================');
      logger.info('Starting ETL Process');
      logger.info('========================================');

      // Pre-flight checks
      const preFlightResult = await ConnectionChecker.preFlightChecks(
        this.sourcePool,
        this.destPool,
        config.source.table,
        config.destination.table
      );

      if (!preFlightResult.passed) {
        throw new Error('Pre-flight checks failed: ' + preFlightResult.errors.join(', '));
      }

      // SQL Query syntax validation
      const selectQuery = config.etl.sqlQuery.replace('{{table}}', config.source.table);
      logger.info('Validating SQL query syntax...');
      const queryValidation = await this.sourcePool.validateQuerySyntax(selectQuery);
      if (!queryValidation.isValid) {
        throw new Error(`SQL query syntax error: ${queryValidation.error}`);
      }
      logger.info('SQL query syntax is valid');

      // Schema compatibility check
      // Use query column metadata for source (supports JOINs and custom columns)
      logger.info('Validating schema compatibility...');
      logger.info('Detecting columns from SQL query result...');
      const sourceSchema = await this.sourcePool.getQueryColumnMetadata(selectQuery);
      const destSchema = await this.destPool.getTableSchema(config.destination.table);
      
      logger.info(`Source query returns ${sourceSchema.length} columns: ${sourceSchema.map(c => c.COLUMN_NAME).join(', ')}`);
      
      const schemaValidation = SchemaValidator.validateSchemaCompatibility(sourceSchema, destSchema);
      if (!schemaValidation.isCompatible) {
        throw new Error('Schema validation failed: ' + schemaValidation.errors.join(', '));
      }

      // Detect or force mode
      let modeResult;
      if (forceMode) {
        modeResult = ModeDetector.forceMode(forceMode);
      } else {
        modeResult = await ModeDetector.detectMode(
          this.sourcePool,
          this.destPool,
          config.source.table,
          config.destination.table
        );
      }

      runResult.mode = modeResult.mode;
      logger.info(`Running in ${modeResult.mode.toUpperCase()} mode: ${modeResult.reason}`);

      // Execute appropriate processor
      let processorResult;
      
      switch (modeResult.mode) {
        case 'full':
          const fullProcessor = new FullLoadProcessor(this.sourcePool, this.destPool);
          processorResult = await fullProcessor.execute();
          runResult.rowsProcessed = processorResult.rowsProcessed;
          runResult.rowsInserted = processorResult.rowsInserted;
          break;

        case 'incremental':
          const incrementalProcessor = new IncrementalLoadProcessor(
            this.sourcePool,
            this.destPool,
            modeResult.details.primaryKeyColumn
          );
          processorResult = await incrementalProcessor.execute();
          runResult.rowsProcessed = processorResult.rowsProcessed;
          runResult.rowsInserted = processorResult.rowsInserted;
          break;

        case 'delta':
          const deltaProcessor = new DeltaLoadProcessor(
            this.sourcePool,
            this.destPool,
            modeResult.details.primaryKeyColumn
          );
          processorResult = await deltaProcessor.execute();
          runResult.rowsProcessed = processorResult.rowsProcessed;
          runResult.rowsDeleted = processorResult.rowsDeleted;
          break;

        default:
          throw new Error(`Unknown mode: ${modeResult.mode}`);
      }

      runResult.status = 'completed';
      logger.info('========================================');
      logger.info('ETL Process Completed Successfully');
      logger.info('========================================');

    } catch (error) {
      runResult.status = 'failed';
      runResult.errorMessage = error.message;
      logger.error(`ETL Process Failed: ${error.message}`);
      logger.error(error.stack);
    } finally {
      const endTime = new Date();
      runResult.endTime = endTime.toISOString();
      runResult.durationSeconds = (endTime - startTime) / 1000;

      // Log run to SQLite history
      try {
        sqliteManager.logETLRun(runResult);
      } catch (logError) {
        logger.error(`Failed to log ETL run to history: ${logError.message}`);
      }

      logger.info(`ETL run duration: ${runResult.durationSeconds.toFixed(2)} seconds`);
      this.isRunning = false;
    }

    return runResult;
  }

  /**
   * Shutdown the ETL runner and close all connections
   */
  async shutdown() {
    logger.info('Shutting down ETL Runner...');

    try {
      if (this.sourcePool) {
        await this.sourcePool.close();
      }
      if (this.destPool) {
        await this.destPool.close();
      }
      sqliteManager.close();

      logger.info('ETL Runner shut down successfully');
    } catch (error) {
      logger.error(`Error during shutdown: ${error.message}`);
    }
  }
}

module.exports = ETLRunner;
