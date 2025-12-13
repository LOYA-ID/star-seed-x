const logger = require('../logger');
const config = require('../config');
const SchemaValidator = require('../utils/schemaValidator');
const sqliteManager = require('../database/sqlite');

/**
 * Full load ETL processor with transaction support, retry logic, and checkpoint/resume
 */
class FullLoadProcessor {
  constructor(sourcePool, destPool) {
    this.sourcePool = sourcePool;
    this.destPool = destPool;
    this.sourceTable = config.source.table;
    this.destTable = config.destination.table;
    this.batchSize = config.etl.batchSize;
    this.primaryKeyColumn = config.etl.primaryKeyColumn;
    this.maxRetries = config.etl.maxRetries;
    this.retryDelay = config.etl.retryDelay;
    this.recordDelay = config.etl.recordDelay;
  }

  /**
   * Execute full load process with transaction support and checkpoint/resume
   * @returns {Object} Processing result
   */
  async execute() {
    const result = {
      rowsProcessed: 0,
      rowsInserted: 0,
      errors: [],
      resumed: false
    };

    logger.info('Starting FULL LOAD process');

    try {
      // Build SQL query
      const baseSelectQuery = config.etl.sqlQuery.replace('{{table}}', this.sourceTable);
      
      // Get columns from the actual query result (supports JOINs and custom columns)
      const columns = await this.sourcePool.getQueryColumns(baseSelectQuery);
      logger.info(`Query columns detected: ${columns.join(', ')}`);
      
      // Check if primary key column exists in query result for seek pagination
      const hasPrimaryKey = columns.includes(this.primaryKeyColumn);
      
      if (!hasPrimaryKey) {
        logger.warn(`Primary key column '${this.primaryKeyColumn}' not found in query. Falling back to OFFSET pagination (slower for large datasets).`);
        logger.warn('Checkpoint/Resume will NOT work without a primary key column!');
      } else {
        logger.info(`Using seek/keyset pagination on '${this.primaryKeyColumn}' for optimal performance`);
      }
      
      // Get total row count for progress tracking
      const totalRows = await this.sourcePool.getRowCount(this.sourceTable);
      logger.info(`Total rows to process: ${totalRows}`);

      // Check for existing checkpoint (resume capability)
      let checkpoint = null;
      let batchNumber = 1;
      let lastPrimaryKeyValue = null;

      if (hasPrimaryKey) {
        checkpoint = sqliteManager.getCheckpoint(this.sourceTable, this.destTable, 'full');
        if (checkpoint) {
          logger.info('========================================');
          logger.info('RESUMING FROM CHECKPOINT');
          logger.info(`Last processed PK: ${checkpoint.last_processed_pk}`);
          logger.info(`Batch number: ${checkpoint.batch_number}`);
          logger.info(`Rows already processed: ${checkpoint.rows_processed}`);
          logger.info(`Rows already inserted: ${checkpoint.rows_inserted}`);
          logger.info('========================================');
          
          lastPrimaryKeyValue = checkpoint.last_processed_pk;
          batchNumber = checkpoint.batch_number + 1;
          result.rowsProcessed = checkpoint.rows_processed;
          result.rowsInserted = checkpoint.rows_inserted;
          result.resumed = true;
        }
      }

      // Process in batches using seek/keyset pagination (much faster than OFFSET)
      while (true) {
        let batchQuery;
        let queryParams = [];

        if (hasPrimaryKey) {
          // Seek/keyset pagination - O(log n) performance
          if (lastPrimaryKeyValue !== null) {
            batchQuery = `${baseSelectQuery} WHERE \`${this.primaryKeyColumn}\` > ? ORDER BY \`${this.primaryKeyColumn}\` ASC LIMIT ${this.batchSize}`;
            queryParams = [lastPrimaryKeyValue];
          } else {
            batchQuery = `${baseSelectQuery} ORDER BY \`${this.primaryKeyColumn}\` ASC LIMIT ${this.batchSize}`;
          }
        } else {
          // Fallback to OFFSET pagination (slower for large datasets)
          const offset = (batchNumber - 1) * this.batchSize;
          batchQuery = `${baseSelectQuery} LIMIT ${this.batchSize} OFFSET ${offset}`;
        }

        logger.debug(`Executing batch ${batchNumber}`);

        // Fetch rows with retry
        const rows = await this.sourcePool.queryWithRetry(batchQuery, queryParams);

        if (rows.length === 0) {
          logger.info('No more rows to process');
          break;
        }

        logger.info(`Processing batch ${batchNumber}: ${rows.length} rows`);

        // Process batch with transaction
        const batchResult = await this.processBatchWithTransaction(rows, columns, hasPrimaryKey);
        
        result.rowsInserted += batchResult.inserted;
        result.rowsProcessed += batchResult.processed;
        result.errors.push(...batchResult.errors);

        // Update last primary key value for seek pagination
        if (hasPrimaryKey && batchResult.lastPk !== null) {
          lastPrimaryKeyValue = batchResult.lastPk;
        }

        // Save checkpoint after each successful batch
        if (hasPrimaryKey) {
          sqliteManager.saveCheckpoint({
            sourceTable: this.sourceTable,
            destinationTable: this.destTable,
            mode: 'full',
            primaryKeyColumn: this.primaryKeyColumn,
            lastProcessedPk: String(lastPrimaryKeyValue),
            batchNumber: batchNumber,
            rowsProcessed: result.rowsProcessed,
            rowsInserted: result.rowsInserted
          });
        }

        // Log progress
        const progress = ((result.rowsProcessed / totalRows) * 100).toFixed(2);
        logger.info(`Progress: ${result.rowsProcessed}/${totalRows} (${progress}%)`);

        batchNumber++;
      }

      // Mark checkpoint as completed
      if (hasPrimaryKey) {
        sqliteManager.completeCheckpoint(this.sourceTable, this.destTable, 'full');
      }

      logger.info(`FULL LOAD completed: ${result.rowsInserted} rows inserted`);
      return result;

    } catch (error) {
      logger.error(`FULL LOAD failed: ${error.message}`);
      throw error;
    }
  }

  /**
   * Process a batch of rows within a transaction
   * @param {Array} rows - Rows to process
   * @param {Array} columns - Column names
   * @param {boolean} hasPrimaryKey - Whether primary key exists
   * @returns {Object} Batch processing result
   */
  async processBatchWithTransaction(rows, columns, hasPrimaryKey) {
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
          
          // Track last primary key value
          if (hasPrimaryKey) {
            batchResult.lastPk = row[this.primaryKeyColumn];
          }

          // Apply delay between records if configured
          if (this.recordDelay > 0) {
            await this.sleep(this.recordDelay);
          }
        } catch (error) {
          // Log error but continue processing other rows
          logger.error(`Error inserting row: ${error.message}`);
          batchResult.errors.push({
            row: row,
            error: error.message
          });
          batchResult.processed++;
          
          // Still track primary key to continue from correct position
          if (hasPrimaryKey) {
            batchResult.lastPk = row[this.primaryKeyColumn];
          }
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
    
    // Use retry logic for transient errors
    await this.destPool.queryInTransactionWithRetry(conn, insertSQL, values);
    logger.debug(`Inserted row with values: ${JSON.stringify(values).substring(0, 100)}...`);
  }

  /**
   * Sleep helper function
   * @param {number} ms - Milliseconds to sleep
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = FullLoadProcessor;
