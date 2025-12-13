const logger = require('../logger');
const config = require('../config');
const SchemaValidator = require('../utils/schemaValidator');
const sqliteManager = require('../database/sqlite');

/**
 * Delta load ETL processor (handles deletions) with transaction support and retry logic
 */
class DeltaLoadProcessor {
  constructor(sourcePool, destPool, primaryKeyColumn) {
    this.sourcePool = sourcePool;
    this.destPool = destPool;
    this.sourceTable = config.source.table;
    this.destTable = config.destination.table;
    this.batchSize = config.etl.batchSize;
    this.primaryKeyColumn = primaryKeyColumn || config.etl.primaryKeyColumn;
    this.deletedFlagColumn = config.etl.deletedFlagColumn;
    this.maxRetries = config.etl.maxRetries || 3;
  }

  /**
   * Execute delta load process with transaction support
   * @returns {Object} Processing result
   */
  async execute() {
    const result = {
      rowsProcessed: 0,
      rowsDeleted: 0,
      errors: []
    };

    logger.info('Starting DELTA LOAD process');
    logger.info(`Using deleted flag column: ${this.deletedFlagColumn}`);
    logger.info(`Using primary key column: ${this.primaryKeyColumn}`);

    try {
      // Find all records marked as deleted in source
      const deleteQuery = `
        SELECT \`${this.primaryKeyColumn}\` 
        FROM \`${this.sourceTable}\` 
        WHERE \`${this.deletedFlagColumn}\` = 1
      `;

      logger.debug(`Fetching deleted records from source`);
      const deletedRecords = await this.sourcePool.queryWithRetry(deleteQuery);

      if (deletedRecords.length === 0) {
        logger.info('No deleted records found in source');
        return result;
      }

      logger.info(`Found ${deletedRecords.length} deleted records to process`);

      // Store deleted record IDs in SQLite for tracking
      for (const record of deletedRecords) {
        sqliteManager.addDeletedRecord(
          this.sourceTable,
          this.destTable,
          String(record[this.primaryKeyColumn])
        );
      }

      // Process deletions in batches with transactions
      let batchNumber = 1;
      const deletedIds = deletedRecords.map(r => r[this.primaryKeyColumn]);

      for (let i = 0; i < deletedIds.length; i += this.batchSize) {
        const batch = deletedIds.slice(i, i + this.batchSize);
        logger.info(`Processing deletion batch ${batchNumber}: ${batch.length} records`);

        // Process batch with transaction
        const batchResult = await this.processBatchWithTransaction(batch);
        
        result.rowsDeleted += batchResult.deleted;
        result.rowsProcessed += batchResult.processed;
        result.errors.push(...batchResult.errors);

        // Mark batch as processed in SQLite
        sqliteManager.markDeletedRecordsProcessed(
          this.sourceTable,
          this.destTable,
          batch.map(String)
        );

        logger.info(`Deletion batch ${batchNumber} completed: ${batchResult.deleted} rows deleted`);
        batchNumber++;
      }

      logger.info(`DELTA LOAD completed: ${result.rowsDeleted} rows deleted`);
      return result;

    } catch (error) {
      logger.error(`DELTA LOAD failed: ${error.message}`);
      throw error;
    }
  }

  /**
   * Process a batch of deletions within a transaction
   * @param {Array} ids - Primary key values to delete
   * @returns {Object} Batch processing result
   */
  async processBatchWithTransaction(ids) {
    const batchResult = {
      processed: 0,
      deleted: 0,
      errors: []
    };

    let conn;
    try {
      // Begin transaction
      conn = await this.destPool.beginTransaction();
      logger.debug('Transaction started for deletion batch');

      // Delete each record within the transaction
      for (const id of ids) {
        try {
          await this.deleteRowInTransaction(conn, id);
          batchResult.deleted++;
          batchResult.processed++;
        } catch (error) {
          logger.error(`Error deleting row with ID ${id}: ${error.message}`);
          batchResult.errors.push({
            id: id,
            error: error.message
          });
          batchResult.processed++;
        }
      }

      // Commit transaction
      await this.destPool.commitTransaction(conn);
      logger.debug(`Transaction committed: ${batchResult.deleted} rows deleted`);

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
   * Delete a single row within a transaction with retry logic
   * @param {Object} conn - Database connection with active transaction
   * @param {*} id - Primary key value
   */
  async deleteRowInTransaction(conn, id) {
    const deleteSQL = SchemaValidator.buildDeleteStatement(
      this.destTable,
      this.primaryKeyColumn
    );

    await this.destPool.queryInTransactionWithRetry(conn, deleteSQL, [id]);
    logger.debug(`Deleted row with PK ${id}`);
  }
}

module.exports = DeltaLoadProcessor;
