const logger = require('../logger');

/**
 * Connection health check utilities
 */
class ConnectionChecker {
  /**
   * Check if all required connections are available
   * @param {MariaDBPool} sourcePool - Source database pool
   * @param {MariaDBPool} destPool - Destination database pool
   * @returns {Object} Connection check result
   */
  static async checkAllConnections(sourcePool, destPool) {
    const result = {
      allConnected: true,
      source: false,
      destination: false,
      errors: []
    };

    // Check source connection
    try {
      result.source = await sourcePool.testConnection();
      if (!result.source) {
        result.errors.push('Source database connection failed');
        result.allConnected = false;
      }
    } catch (error) {
      result.errors.push(`Source database error: ${error.message}`);
      result.allConnected = false;
    }

    // Check destination connection
    try {
      result.destination = await destPool.testConnection();
      if (!result.destination) {
        result.errors.push('Destination database connection failed');
        result.allConnected = false;
      }
    } catch (error) {
      result.errors.push(`Destination database error: ${error.message}`);
      result.allConnected = false;
    }

    // Log results
    if (result.allConnected) {
      logger.info('All database connections verified successfully');
    } else {
      result.errors.forEach(err => logger.error(err));
    }

    return result;
  }

  /**
   * Check if tables exist
   * @param {MariaDBPool} sourcePool - Source database pool
   * @param {MariaDBPool} destPool - Destination database pool
   * @param {string} sourceTable - Source table name
   * @param {string} destTable - Destination table name
   * @returns {Object} Table check result
   */
  static async checkTables(sourcePool, destPool, sourceTable, destTable) {
    const result = {
      allTablesExist: true,
      sourceTableExists: false,
      destTableExists: false,
      errors: []
    };

    // Check source table
    try {
      result.sourceTableExists = await sourcePool.tableExists(sourceTable);
      if (!result.sourceTableExists) {
        result.errors.push(`Source table '${sourceTable}' does not exist`);
        result.allTablesExist = false;
      } else {
        logger.info(`Source table '${sourceTable}' verified`);
      }
    } catch (error) {
      result.errors.push(`Error checking source table: ${error.message}`);
      result.allTablesExist = false;
    }

    // Check destination table
    try {
      result.destTableExists = await destPool.tableExists(destTable);
      if (!result.destTableExists) {
        result.errors.push(`Destination table '${destTable}' does not exist`);
        result.allTablesExist = false;
      } else {
        logger.info(`Destination table '${destTable}' verified`);
      }
    } catch (error) {
      result.errors.push(`Error checking destination table: ${error.message}`);
      result.allTablesExist = false;
    }

    // Log errors
    result.errors.forEach(err => logger.error(err));

    return result;
  }

  /**
   * Perform comprehensive pre-flight checks
   * @param {MariaDBPool} sourcePool - Source database pool
   * @param {MariaDBPool} destPool - Destination database pool
   * @param {string} sourceTable - Source table name
   * @param {string} destTable - Destination table name
   * @returns {Object} Pre-flight check result
   */
  static async preFlightChecks(sourcePool, destPool, sourceTable, destTable) {
    logger.info('Starting pre-flight checks...');

    const result = {
      passed: true,
      connectionCheck: null,
      tableCheck: null,
      errors: []
    };

    // Check connections
    result.connectionCheck = await this.checkAllConnections(sourcePool, destPool);
    if (!result.connectionCheck.allConnected) {
      result.passed = false;
      result.errors.push(...result.connectionCheck.errors);
      return result;
    }

    // Check tables
    result.tableCheck = await this.checkTables(sourcePool, destPool, sourceTable, destTable);
    if (!result.tableCheck.allTablesExist) {
      result.passed = false;
      result.errors.push(...result.tableCheck.errors);
      return result;
    }

    logger.info('All pre-flight checks passed');
    return result;
  }
}

module.exports = ConnectionChecker;
