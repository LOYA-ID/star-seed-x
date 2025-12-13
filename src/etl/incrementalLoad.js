const logger = require('../logger');
const config = require('../config');
const SchemaValidator = require('../utils/schemaValidator');
const sqliteManager = require('../database/sqlite');

/**
 * Incremental load ETL processor
 */
class IncrementalLoadProcessor {
  constructor(sourcePool, destPool, primaryKeyColumn) {
    this.sourcePool = sourcePool;
    this.destPool = destPool;
    this.sourceTable = config.source.table;
    this.destTable = config.destination.table;
    this.batchSize = config.etl.batchSize;
    this.primaryKeyColumn = primaryKeyColumn || config.etl.primaryKeyColumn;
  }

  /**
   * Execute incremental load process
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

        const rows = await this.sourcePool.query(batchQuery, queryParams);

        if (rows.length === 0) {
          logger.info('No more new rows to process');
          break;
        }

        logger.info(`Processing batch ${batchNumber}: ${rows.length} rows`);

        // Process each row individually
        for (const row of rows) {
          try {
            await this.insertRow(row, columns);
            result.rowsInserted++;
            result.rowsProcessed++;

            // Track the last processed primary key value
            currentLastValue = row[this.primaryKeyColumn];
          } catch (error) {
            logger.error(`Error inserting row: ${error.message}`);
            result.errors.push({
              row: row,
              error: error.message
            });
            result.rowsProcessed++;
          }
        }

        // Update last processed value after each batch
        if (currentLastValue !== null) {
          sqliteManager.updateLastProcessedValue(
            this.sourceTable,
            this.destTable,
            this.primaryKeyColumn,
            String(currentLastValue)
          );
        }

        logger.info(`Batch ${batchNumber} completed: ${rows.length} rows processed`);

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
   * Insert a single row into destination table
   * @param {Object} row - Row data
   * @param {Array} columns - Column names
   */
  async insertRow(row, columns) {
    const insertSQL = SchemaValidator.buildInsertStatement(this.destTable, columns);
    const values = columns.map(col => row[col]);

    await this.destPool.query(insertSQL, values);
    logger.debug(`Inserted row with PK ${row[this.primaryKeyColumn]}`);
  }
}

module.exports = IncrementalLoadProcessor;
