const logger = require('../logger');
const config = require('../config');
const SchemaValidator = require('../utils/schemaValidator');
const sqliteManager = require('../database/sqlite');

/**
 * Delta load ETL processor (handles deletions)
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
  }

  /**
   * Execute delta load process
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
      const deletedRecords = await this.sourcePool.query(deleteQuery);

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

      // Process deletions in batches
      let batchNumber = 1;
      const deletedIds = deletedRecords.map(r => r[this.primaryKeyColumn]);

      for (let i = 0; i < deletedIds.length; i += this.batchSize) {
        const batch = deletedIds.slice(i, i + this.batchSize);
        logger.info(`Processing deletion batch ${batchNumber}: ${batch.length} records`);

        // Delete each record individually
        for (const id of batch) {
          try {
            await this.deleteRow(id);
            result.rowsDeleted++;
            result.rowsProcessed++;
          } catch (error) {
            logger.error(`Error deleting row with ID ${id}: ${error.message}`);
            result.errors.push({
              id: id,
              error: error.message
            });
            result.rowsProcessed++;
          }
        }

        // Mark batch as processed in SQLite
        sqliteManager.markDeletedRecordsProcessed(
          this.sourceTable,
          this.destTable,
          batch.map(String)
        );

        logger.info(`Deletion batch ${batchNumber} completed`);
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
   * Delete a single row from destination table
   * @param {*} id - Primary key value
   */
  async deleteRow(id) {
    const deleteSQL = SchemaValidator.buildDeleteStatement(
      this.destTable,
      this.primaryKeyColumn
    );

    await this.destPool.query(deleteSQL, [id]);
    logger.debug(`Deleted row with PK ${id}`);
  }
}

module.exports = DeltaLoadProcessor;
