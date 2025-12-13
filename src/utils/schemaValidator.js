const logger = require('../logger');

/**
 * Schema validation utilities
 */
class SchemaValidator {
  /**
   * Compare source and destination table schemas
   * @param {Array} sourceSchema - Source table schema
   * @param {Array} destSchema - Destination table schema
   * @param {string} primaryKeyColumn - Primary key column name (optional)
   * @returns {Object} Validation result
   */
  static validateSchemaCompatibility(sourceSchema, destSchema, primaryKeyColumn = null) {
    const result = {
      isCompatible: true,
      errors: [],
      warnings: []
    };

    // Create maps for easy lookup
    const sourceColumns = new Map(sourceSchema.map(col => [col.COLUMN_NAME, col]));
    const destColumns = new Map(destSchema.map(col => [col.COLUMN_NAME, col]));

    // Check if all source columns exist in destination
    for (const [colName, sourceCol] of sourceColumns) {
      if (!destColumns.has(colName)) {
        result.errors.push(`Column '${colName}' exists in source but not in destination`);
        result.isCompatible = false;
        continue;
      }

      const destCol = destColumns.get(colName);

      // Check data type compatibility
      if (!this.areTypesCompatible(sourceCol.DATA_TYPE, destCol.DATA_TYPE)) {
        result.warnings.push(
          `Column '${colName}' has different data types: source='${sourceCol.DATA_TYPE}', destination='${destCol.DATA_TYPE}'`
        );
      }

      // Check nullability
      if (sourceCol.IS_NULLABLE === 'YES' && destCol.IS_NULLABLE === 'NO') {
        result.warnings.push(
          `Column '${colName}' allows NULL in source but not in destination`
        );
      }
    }

    // Check for extra columns in destination (just a warning)
    for (const colName of destColumns.keys()) {
      if (!sourceColumns.has(colName)) {
        result.warnings.push(`Column '${colName}' exists in destination but not in source`);
      }
    }

    // Validate primary key in destination
    if (primaryKeyColumn) {
      const pkValidation = this.validateDestinationPrimaryKey(destSchema, primaryKeyColumn);
      if (!pkValidation.isValid) {
        result.errors.push(...pkValidation.errors);
        result.isCompatible = false;
      }
      result.warnings.push(...pkValidation.warnings);
    }

    // Log validation results
    if (result.isCompatible) {
      logger.info('Schema validation passed');
    } else {
      logger.error('Schema validation failed');
    }

    result.errors.forEach(err => logger.error(`Schema error: ${err}`));
    result.warnings.forEach(warn => logger.warn(`Schema warning: ${warn}`));

    return result;
  }

  /**
   * Validate destination table primary key configuration
   * @param {Array} destSchema - Destination table schema
   * @param {string} primaryKeyColumn - Expected primary key column name
   * @returns {Object} Validation result
   */
  static validateDestinationPrimaryKey(destSchema, primaryKeyColumn) {
    const result = {
      isValid: true,
      errors: [],
      warnings: []
    };

    // Find the primary key column in destination schema
    const pkColumn = destSchema.find(col => col.COLUMN_NAME === primaryKeyColumn);

    if (!pkColumn) {
      result.errors.push(
        `Primary key column '${primaryKeyColumn}' not found in destination table`
      );
      result.isValid = false;
      return result;
    }

    // Check if destination primary key column is actually a PRIMARY KEY
    if (pkColumn.COLUMN_KEY !== 'PRI') {
      result.errors.push(
        `Column '${primaryKeyColumn}' is not a PRIMARY KEY in destination table. ` +
        `Expected COLUMN_KEY='PRI', got '${pkColumn.COLUMN_KEY || 'none'}'`
      );
      result.isValid = false;
    }

    // Check if destination primary key has AUTO_INCREMENT (should NOT have it)
    const extra = (pkColumn.EXTRA || '').toLowerCase();
    if (extra.includes('auto_increment')) {
      result.errors.push(
        `Destination primary key '${primaryKeyColumn}' has AUTO_INCREMENT. ` +
        `This will cause conflicts when inserting data from source. ` +
        `Please remove AUTO_INCREMENT from destination table.`
      );
      result.isValid = false;
    }

    // Log validation info
    if (result.isValid) {
      logger.info(`Destination primary key '${primaryKeyColumn}' validated successfully`);
    }

    return result;
  }

  /**
   * Check if two data types are compatible
   * @param {string} sourceType - Source column data type
   * @param {string} destType - Destination column data type
   * @returns {boolean} Whether types are compatible
   */
  static areTypesCompatible(sourceType, destType) {
    // Normalize type names
    const normalizedSource = sourceType.toLowerCase();
    const normalizedDest = destType.toLowerCase();

    // Exact match
    if (normalizedSource === normalizedDest) {
      return true;
    }

    // Compatible type groups
    const typeGroups = [
      ['int', 'integer', 'bigint', 'smallint', 'tinyint', 'mediumint'],
      ['varchar', 'char', 'text', 'longtext', 'mediumtext', 'tinytext'],
      ['decimal', 'numeric', 'float', 'double', 'real'],
      ['datetime', 'timestamp'],
      ['date'],
      ['time'],
      ['blob', 'longblob', 'mediumblob', 'tinyblob', 'binary', 'varbinary']
    ];

    for (const group of typeGroups) {
      const sourceInGroup = group.some(t => normalizedSource.includes(t));
      const destInGroup = group.some(t => normalizedDest.includes(t));
      if (sourceInGroup && destInGroup) {
        return true;
      }
    }

    return false;
  }

  /**
   * Get column names from schema
   * @param {Array} schema - Table schema
   * @returns {Array} Column names
   */
  static getColumnNames(schema) {
    return schema.map(col => col.COLUMN_NAME);
  }

  /**
   * Build INSERT statement for destination table
   * @param {string} tableName - Destination table name
   * @param {Array} columns - Column names
   * @returns {string} INSERT SQL statement
   */
  static buildInsertStatement(tableName, columns) {
    const columnList = columns.map(c => `\`${c}\``).join(', ');
    const placeholders = columns.map(() => '?').join(', ');
    return `INSERT INTO \`${tableName}\` (${columnList}) VALUES (${placeholders})`;
  }

  /**
   * Build DELETE statement for destination table
   * @param {string} tableName - Destination table name
   * @param {string} primaryKeyColumn - Primary key column name
   * @returns {string} DELETE SQL statement
   */
  static buildDeleteStatement(tableName, primaryKeyColumn) {
    return `DELETE FROM \`${tableName}\` WHERE \`${primaryKeyColumn}\` = ?`;
  }
}

module.exports = SchemaValidator;
