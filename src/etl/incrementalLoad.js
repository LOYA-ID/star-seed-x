const logger = require('../logger');
const config = require('../config');
const SchemaValidator = require('../utils/schemaValidator');
const sqliteManager = require('../database/sqlite');

/**
 * Incremental load ETL processor with transaction support and retry logic
 */
class IncrementalLoadProcessor {
  constructor(sourcePool, destPool, primaryKeyColumn) {
    this.sourcePool = sourcePool;
    this.destPool = destPool;
    this.sourceTable = config.source.table;
    this.destTable = config.destination.table;
    this.batchSize = config.etl.batchSize;
    this.primaryKeyColumn = primaryKeyColumn || config.etl.primaryKeyColumn;
    this.maxRetries = config.etl.maxRetries;
    this.recordDelay = config.etl.recordDelay;
  }

  /**
   * Execute incremental load process with transaction support
   * @returns {Object} Processing result
   */
  async execute() {
    const result = {
      rowsProcessed: 0,
      rowsInserted: 0,
      errors: [],
      lastProcessedValue: null
    };

    logger.info('Starting INCREMENTAL LOAD process');
    logger.info(`Using primary key column: ${this.primaryKeyColumn}`);

    try {
      // Build base query
      const baseSelectQuery = config.etl.sqlQuery.replace('{{table}}', this.sourceTable);
      
      // Get columns from the actual query result (supports JOINs and custom columns)
      const columns = await this.sourcePool.getQueryColumns(baseSelectQuery);
      logger.info(`Query columns detected: ${columns.join(', ')}`);

      // Get last processed value from SQLite
      const lastValue = sqliteManager.getLastProcessedValue(
        this.sourceTable,
        this.destTable,
        this.primaryKeyColumn
      );

      logger.info(`Last processed value: ${lastValue || 'None (first run)'}`);

      // Build query with WHERE clause for incremental
      let baseQuery = baseSelectQuery;
      
      // Add ORDER BY to ensure consistent processing
      baseQuery += ` ORDER BY \`${this.primaryKeyColumn}\` ASC`;

      // Process in batches using cursor-based pagination (more efficient than OFFSET)
      let batchNumber = 1;
      let currentLastValue = lastValue;

      while (true) {
        let batchQuery;
        let queryParams = [];

        if (currentLastValue !== null) {
          // Add WHERE clause for incremental processing
          if (baseQuery.toLowerCase().includes('where')) {
            batchQuery = baseQuery.replace(/ORDER BY/i, `AND \`${this.primaryKeyColumn}\` > ? ORDER BY`);
          } else {
            batchQuery = baseQuery.replace(/ORDER BY/i, `WHERE \`${this.primaryKeyColumn}\` > ? ORDER BY`);
          }
          batchQuery += ` LIMIT ${this.batchSize}`;
          queryParams = [currentLastValue];
        } else {
          batchQuery = `${baseQuery} LIMIT ${this.batchSize}`;
        }

        logger.debug(`Executing batch ${batchNumber}`);

        // Fetch rows with retry
        const rows = await this.sourcePool.queryWithRetry(batchQuery, queryParams);

        if (rows.length === 0) {
          logger.info('No more new rows to process');
          break;
        }

        logger.info(`Processing batch ${batchNumber}: ${rows.length} rows`);

        // Process batch with transaction
        const batchResult = await this.processBatchWithTransaction(rows, columns);
        
        result.rowsInserted += batchResult.inserted;
        result.rowsProcessed += batchResult.processed;
        result.errors.push(...batchResult.errors);

        // Update current last value
        if (batchResult.lastPk !== null) {
          currentLastValue = batchResult.lastPk;
        }

        // Update last processed value after each batch (checkpoint)
        if (currentLastValue !== null) {
          sqliteManager.updateLastProcessedValue(
            this.sourceTable,
            this.destTable,
            this.primaryKeyColumn,
            String(currentLastValue)
          );
        }

        logger.info(`Batch ${batchNumber} completed: ${batchResult.inserted} rows inserted`);

        batchNumber++;
      }

      result.lastProcessedValue = currentLastValue;
      logger.info(`INCREMENTAL LOAD completed: ${result.rowsInserted} rows inserted`);
      return result;

    } catch (error) {
      logger.error(`INCREMENTAL LOAD failed: ${error.message}`);
      throw error;
    }
  }

  /**
   * Process a batch of rows within a transaction
   * @param {Array} rows - Rows to process
   * @param {Array} columns - Column names
   * @returns {Object} Batch processing result
   */
  async processBatchWithTransaction(rows, columns) {
    const batchResult = {
      processed: 0,
      inserted: 0,
      errors: [],
      lastPk: null
    };

    let conn;
    try {
      // Begin transaction
      conn = await this.destPool.beginTransaction();
      logger.debug('Transaction started for batch');

      // Process each row within the transaction
      for (const row of rows) {
        try {
          await this.insertRowInTransaction(conn, row, columns);
          batchResult.inserted++;
          batchResult.processed++;

          // Track the last processed primary key value
          batchResult.lastPk = row[this.primaryKeyColumn];

          // Apply delay between records if configured
          if (this.recordDelay > 0) {
            await this.sleep(this.recordDelay);
          }
        } catch (error) {
          logger.error(`Error inserting row: ${error.message}`);
          batchResult.errors.push({
            row: row,
            error: error.message
          });
          batchResult.processed++;
          
          // Still track primary key to continue
          batchResult.lastPk = row[this.primaryKeyColumn];
        }
      }

      // Commit transaction
      await this.destPool.commitTransaction(conn);
      logger.debug(`Transaction committed: ${batchResult.inserted} rows inserted`);

    } catch (error) {
      // Rollback on error
      if (conn) {
        try {
          await this.destPool.rollbackTransaction(conn);
          logger.warn('Transaction rolled back due to error');
        } catch (rollbackError) {
          logger.error(`Rollback failed: ${rollbackError.message}`);
        }
      }
      throw error;
    }

    return batchResult;
  }

  /**
   * Insert a single row within a transaction with retry logic
   * @param {Object} conn - Database connection with active transaction
   * @param {Object} row - Row data
   * @param {Array} columns - Column names
   */
  async insertRowInTransaction(conn, row, columns) {
    const insertSQL = SchemaValidator.buildInsertStatement(this.destTable, columns);
    const values = columns.map(col => row[col]);

    await this.destPool.queryInTransactionWithRetry(conn, insertSQL, values);
    logger.debug(`Inserted row with PK ${row[this.primaryKeyColumn]}`);
  }

  /**
   * Sleep helper function
   * @param {number} ms - Milliseconds to sleep
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = IncrementalLoadProcessor;
