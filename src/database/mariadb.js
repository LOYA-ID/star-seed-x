const mariadb = require('mariadb');
const logger = require('../logger');

/**
 * MariaDB connection pool manager
 */
class MariaDBPool {
  constructor(name, config) {
    this.name = name;
    this.config = config;
    this.pool = null;
  }

  /**
   * Initialize connection pool
   */
  async initialize() {
    try {
      this.pool = mariadb.createPool({
        host: this.config.host,
        port: this.config.port,
        user: this.config.user,
        password: this.config.password,
        database: this.config.database,
        connectionLimit: this.config.connectionPoolSize,
        acquireTimeout: 30000,
        idleTimeout: 60000,
        minimumIdle: 1,
        bigIntAsNumber: true,  // Convert BigInt to Number
        insertIdAsNumber: true // Convert insert ID to Number
      });

      logger.info(`${this.name} MariaDB pool initialized successfully`);
      return true;
    } catch (error) {
      logger.error(`Failed to initialize ${this.name} MariaDB pool: ${error.message}`);
      throw error;
    }
  }

  /**
   * Get a connection from the pool
   */
  async getConnection() {
    if (!this.pool) {
      throw new Error(`${this.name} pool not initialized`);
    }
    return await this.pool.getConnection();
  }

  /**
   * Execute a query using a connection from the pool
   */
  async query(sql, params = []) {
    let conn;
    try {
      conn = await this.getConnection();
      const result = await conn.query(sql, params);
      return result;
    } finally {
      if (conn) {
        conn.release();
      }
    }
  }

  /**
   * Test connection to the database
   */
  async testConnection() {
    let conn;
    try {
      conn = await this.getConnection();
      await conn.query('SELECT 1');
      logger.info(`${this.name} connection test successful`);
      return true;
    } catch (error) {
      logger.error(`${this.name} connection test failed: ${error.message}`);
      return false;
    } finally {
      if (conn) {
        conn.release();
      }
    }
  }

  /**
   * Get table schema information
   */
  async getTableSchema(tableName) {
    const sql = `
      SELECT 
        COLUMN_NAME, 
        DATA_TYPE, 
        IS_NULLABLE, 
        COLUMN_KEY,
        COLUMN_DEFAULT,
        EXTRA
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
      ORDER BY ORDINAL_POSITION
    `;
    return await this.query(sql, [this.config.database, tableName]);
  }

  /**
   * Check if table exists
   */
  async tableExists(tableName) {
    const sql = `
      SELECT COUNT(*) as count 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    `;
    const result = await this.query(sql, [this.config.database, tableName]);
    return result[0].count > 0;
  }

  /**
   * Get row count of a table
   */
  async getRowCount(tableName) {
    const sql = `SELECT COUNT(*) as count FROM \`${tableName}\``;
    const result = await this.query(sql);
    return Number(result[0].count);
  }

  /**
   * Check if column exists in table
   */
  async columnExists(tableName, columnName) {
    const schema = await this.getTableSchema(tableName);
    return schema.some(col => col.COLUMN_NAME === columnName);
  }

  /**
   * Get primary key column(s) of a table
   */
  async getPrimaryKeyColumns(tableName) {
    const schema = await this.getTableSchema(tableName);
    return schema
      .filter(col => col.COLUMN_KEY === 'PRI')
      .map(col => col.COLUMN_NAME);
  }

  /**
   * Validate SQL query syntax by running EXPLAIN
   * @param {string} sql - SQL query to validate
   * @returns {Object} Validation result
   */
  async validateQuerySyntax(sql) {
    const result = {
      isValid: true,
      error: null
    };

    try {
      // Use EXPLAIN to validate query syntax without executing
      await this.query(`EXPLAIN ${sql}`);
      logger.debug('SQL query syntax validation passed');
    } catch (error) {
      result.isValid = false;
      result.error = error.message;
      logger.error(`SQL query syntax validation failed: ${error.message}`);
    }

    return result;
  }

  /**
   * Get column names from a SQL query result (runs query with LIMIT 0)
   * @param {string} sql - SQL query
   * @returns {Array} Column names from the query result
   */
  async getQueryColumns(sql) {
    let conn;
    try {
      conn = await this.getConnection();
      
      // Run query with LIMIT 0 to get column metadata without fetching data
      const limitedSql = `${sql} LIMIT 0`;
      const result = await conn.query(limitedSql);
      
      // Get column names from result metadata
      if (result.meta && Array.isArray(result.meta)) {
        return result.meta.map(col => col.name());
      }
      
      // Fallback: get keys from empty result or first row structure
      return [];
    } catch (error) {
      logger.error(`Failed to get query columns: ${error.message}`);
      throw error;
    } finally {
      if (conn) {
        conn.release();
      }
    }
  }

  /**
   * Get column metadata from a SQL query result
   * @param {string} sql - SQL query
   * @returns {Array} Column metadata array similar to table schema
   */
  async getQueryColumnMetadata(sql) {
    let conn;
    try {
      conn = await this.getConnection();
      
      // Run query with LIMIT 0 to get column metadata
      const limitedSql = `${sql} LIMIT 0`;
      const result = await conn.query(limitedSql);
      
      // Convert result metadata to schema-like format
      if (result.meta && Array.isArray(result.meta)) {
        return result.meta.map(col => ({
          COLUMN_NAME: col.name(),
          DATA_TYPE: this.mapMariaDBType(col.type),
          IS_NULLABLE: 'YES', // Cannot determine from query result
          COLUMN_KEY: ''
        }));
      }
      
      return [];
    } catch (error) {
      logger.error(`Failed to get query column metadata: ${error.message}`);
      throw error;
    } finally {
      if (conn) {
        conn.release();
      }
    }
  }

  /**
   * Map MariaDB type code to type name
   * @param {number} typeCode - MariaDB type code
   * @returns {string} Type name
   */
  mapMariaDBType(typeCode) {
    // MariaDB type codes mapping
    const typeMap = {
      0: 'decimal',
      1: 'tinyint',
      2: 'smallint',
      3: 'int',
      4: 'float',
      5: 'double',
      6: 'null',
      7: 'timestamp',
      8: 'bigint',
      9: 'mediumint',
      10: 'date',
      11: 'time',
      12: 'datetime',
      13: 'year',
      14: 'date',
      15: 'varchar',
      16: 'bit',
      245: 'json',
      246: 'decimal',
      247: 'enum',
      248: 'set',
      249: 'tinyblob',
      250: 'mediumblob',
      251: 'longblob',
      252: 'blob',
      253: 'varchar',
      254: 'char',
      255: 'geometry'
    };
    return typeMap[typeCode] || 'unknown';
  }

  /**
   * Close the connection pool
   */
  async close() {
    if (this.pool) {
      await this.pool.end();
      logger.info(`${this.name} MariaDB pool closed`);
      this.pool = null;
    }
  }
}

module.exports = MariaDBPool;
