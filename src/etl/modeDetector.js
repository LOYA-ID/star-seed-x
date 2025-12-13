const logger = require('../logger');
const config = require('../config');

/**
 * ETL mode detection
 */
class ModeDetector {
  /**
   * Detect which ETL mode to use based on source and destination state
   * @param {MariaDBPool} sourcePool - Source database pool
   * @param {MariaDBPool} destPool - Destination database pool
   * @param {string} sourceTable - Source table name
   * @param {string} destTable - Destination table name
   * @returns {Object} Detection result with mode and details
   */
  static async detectMode(sourcePool, destPool, sourceTable, destTable) {
    const result = {
      mode: null,
      reason: '',
      details: {}
    };

    try {
      // Check destination table row count
      const destRowCount = await destPool.getRowCount(destTable);
      logger.debug(`Destination table has ${destRowCount} rows`);

      // If destination is empty, run full load
      if (destRowCount === 0) {
        result.mode = 'full';
        result.reason = 'Destination table is empty';
        logger.info(`Mode detected: FULL LOAD - ${result.reason}`);
        return result;
      }

      // Check for deleted flag column in source table
      const hasDeletedFlag = await sourcePool.columnExists(
        sourceTable,
        config.etl.deletedFlagColumn
      );

      // Get primary key columns first (needed for both delta and incremental)
      const primaryKeyColumns = await sourcePool.getPrimaryKeyColumns(sourceTable);
      const pkColumn = primaryKeyColumns.length > 0
        ? (primaryKeyColumns.includes(config.etl.primaryKeyColumn)
          ? config.etl.primaryKeyColumn
          : primaryKeyColumns[0])
        : config.etl.primaryKeyColumn;

      if (hasDeletedFlag) {
        // Check if there are deleted records
        const deletedCount = await this.getDeletedRecordCount(
          sourcePool,
          sourceTable
        );
        
        if (deletedCount > 0) {
          result.mode = 'delta';
          result.reason = `Found ${deletedCount} deleted records in source`;
          result.details.deletedCount = deletedCount;
          result.details.primaryKeyColumn = pkColumn;
          logger.info(`Mode detected: DELTA LOAD - ${result.reason}`);
          return result;
        }
      }

      // Check for primary key column for incremental load
      if (primaryKeyColumns.length > 0) {
        result.mode = 'incremental';
        result.reason = `Primary key column '${pkColumn}' found`;
        result.details.primaryKeyColumn = pkColumn;
        logger.info(`Mode detected: INCREMENTAL LOAD - ${result.reason}`);
        return result;
      }

      // Default to full load if no other criteria met
      result.mode = 'full';
      result.reason = 'No primary key found, defaulting to full load';
      logger.info(`Mode detected: FULL LOAD - ${result.reason}`);
      return result;

    } catch (error) {
      logger.error(`Error detecting ETL mode: ${error.message}`);
      throw error;
    }
  }

  /**
   * Get count of deleted records in source table
   * @param {MariaDBPool} sourcePool - Source database pool
   * @param {string} sourceTable - Source table name
   * @returns {number} Count of deleted records
   */
  static async getDeletedRecordCount(sourcePool, sourceTable) {
    const sql = `SELECT COUNT(*) as count FROM \`${sourceTable}\` WHERE \`${config.etl.deletedFlagColumn}\` = 1`;
    const result = await sourcePool.query(sql);
    return Number(result[0].count);
  }

  /**
   * Force a specific mode (for manual override)
   * @param {string} mode - Mode to force ('full', 'incremental', 'delta')
   * @returns {Object} Mode result
   */
  static forceMode(mode) {
    const validModes = ['full', 'incremental', 'delta'];
    if (!validModes.includes(mode)) {
      throw new Error(`Invalid mode: ${mode}. Valid modes are: ${validModes.join(', ')}`);
    }

    logger.info(`Mode forced: ${mode.toUpperCase()} LOAD`);
    return {
      mode,
      reason: 'Manually forced',
      details: {}
    };
  }
}

module.exports = ModeDetector;
