const logger = require('../logger');
const config = require('../config');
const SchemaValidator = require('../utils/schemaValidator');

/**
 * Full load ETL processor
 */
class FullLoadProcessor {
  constructor(sourcePool, destPool) {
    this.sourcePool = sourcePool;
    this.destPool = destPool;
    this.sourceTable = config.source.table;
    this.destTable = config.destination.table;
    this.batchSize = config.etl.batchSize;
    this.primaryKeyColumn = config.etl.primaryKeyColumn;
  }

  /**
   * Execute full load process
   * @returns {Object} Processing result
   */
  async execute() {
    const result = {
      rowsProcessed: 0,
      rowsInserted: 0,
      errors: []
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
      } else {
        logger.info(`Using seek/keyset pagination on '${this.primaryKeyColumn}' for optimal performance`);
      }
      
      // Get total row count for progress tracking
      const totalRows = await this.sourcePool.getRowCount(this.sourceTable);
      logger.info(`Total rows to process: ${totalRows}`);

      // Process in batches using seek/keyset pagination (much faster than OFFSET)
      let batchNumber = 1;
      let lastPrimaryKeyValue = null;

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

        const rows = await this.sourcePool.query(batchQuery, queryParams);

        if (rows.length === 0) {
          logger.info('No more rows to process');
          break;
        }

        logger.info(`Processing batch ${batchNumber}: ${rows.length} rows`);

        // Process each row individually
        for (const row of rows) {
          try {
            await this.insertRow(row, columns);
            result.rowsInserted++;
            result.rowsProcessed++;
            
            // Track last primary key value for seek pagination
            if (hasPrimaryKey) {
              lastPrimaryKeyValue = row[this.primaryKeyColumn];
            }
          } catch (error) {
            logger.error(`Error inserting row: ${error.message}`);
            result.errors.push({
              row: row,
              error: error.message
            });
            result.rowsProcessed++;
            
            // Still track primary key even on error to continue processing
            if (hasPrimaryKey) {
              lastPrimaryKeyValue = row[this.primaryKeyColumn];
            }
          }
        }

        // Log progress
        const progress = ((result.rowsProcessed / totalRows) * 100).toFixed(2);
        logger.info(`Progress: ${result.rowsProcessed}/${totalRows} (${progress}%)`);

        batchNumber++;
      }

      logger.info(`FULL LOAD completed: ${result.rowsInserted} rows inserted`);
      return result;

    } catch (error) {
      logger.error(`FULL LOAD failed: ${error.message}`);
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
    logger.debug(`Inserted row with values: ${JSON.stringify(values).substring(0, 100)}...`);
  }
}

module.exports = FullLoadProcessor;
