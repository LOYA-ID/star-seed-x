const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const logger = require('../logger');
const config = require('../config');

/**
 * SQLite database manager for ETL state tracking
 */
class SQLiteManager {
  constructor() {
    this.db = null;
  }

  /**
   * Initialize SQLite database
   */
  initialize() {
    try {
      // Ensure directory exists
      const dbDir = path.dirname(config.sqlite.dbPath);
      if (!fs.existsSync(dbDir)) {
        fs.mkdirSync(dbDir, { recursive: true });
      }

      this.db = new Database(config.sqlite.dbPath);
      this.createTables();
      logger.info('SQLite database initialized successfully');
      return true;
    } catch (error) {
      logger.error(`Failed to initialize SQLite database: ${error.message}`);
      throw error;
    }
  }

  /**
   * Create necessary tables for state tracking
   */
  createTables() {
    // Table for tracking last processed primary key for incremental loads
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS incremental_state (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_table TEXT NOT NULL,
        destination_table TEXT NOT NULL,
        primary_key_column TEXT NOT NULL,
        last_processed_value TEXT,
        last_run_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source_table, destination_table, primary_key_column)
      )
    `);

    // Table for tracking deleted records for delta loads
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS deleted_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_table TEXT NOT NULL,
        destination_table TEXT NOT NULL,
        record_id TEXT NOT NULL,
        deleted_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        processed INTEGER DEFAULT 0,
        UNIQUE(source_table, destination_table, record_id)
      )
    `);

    // Table for ETL run history
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS etl_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_table TEXT NOT NULL,
        destination_table TEXT NOT NULL,
        mode TEXT NOT NULL,
        rows_processed INTEGER DEFAULT 0,
        rows_inserted INTEGER DEFAULT 0,
        rows_deleted INTEGER DEFAULT 0,
        status TEXT NOT NULL,
        error_message TEXT,
        start_time DATETIME,
        end_time DATETIME,
        duration_seconds REAL
      )
    `);

    logger.debug('SQLite tables created/verified');
  }

  /**
   * Get last processed primary key value
   */
  getLastProcessedValue(sourceTable, destinationTable, primaryKeyColumn) {
    const stmt = this.db.prepare(`
      SELECT last_processed_value 
      FROM incremental_state 
      WHERE source_table = ? AND destination_table = ? AND primary_key_column = ?
    `);
    const result = stmt.get(sourceTable, destinationTable, primaryKeyColumn);
    return result ? result.last_processed_value : null;
  }

  /**
   * Update last processed primary key value
   */
  updateLastProcessedValue(sourceTable, destinationTable, primaryKeyColumn, value) {
    const stmt = this.db.prepare(`
      INSERT INTO incremental_state (source_table, destination_table, primary_key_column, last_processed_value, last_run_timestamp)
      VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(source_table, destination_table, primary_key_column) 
      DO UPDATE SET last_processed_value = ?, last_run_timestamp = CURRENT_TIMESTAMP
    `);
    stmt.run(sourceTable, destinationTable, primaryKeyColumn, value, value);
    logger.debug(`Updated last processed value to ${value}`);
  }

  /**
   * Add deleted record for delta processing
   */
  addDeletedRecord(sourceTable, destinationTable, recordId) {
    const stmt = this.db.prepare(`
      INSERT OR IGNORE INTO deleted_records (source_table, destination_table, record_id)
      VALUES (?, ?, ?)
    `);
    stmt.run(sourceTable, destinationTable, recordId);
  }

  /**
   * Get unprocessed deleted records
   */
  getUnprocessedDeletedRecords(sourceTable, destinationTable) {
    const stmt = this.db.prepare(`
      SELECT record_id 
      FROM deleted_records 
      WHERE source_table = ? AND destination_table = ? AND processed = 0
    `);
    return stmt.all(sourceTable, destinationTable).map(r => r.record_id);
  }

  /**
   * Mark deleted records as processed
   */
  markDeletedRecordsProcessed(sourceTable, destinationTable, recordIds) {
    const stmt = this.db.prepare(`
      UPDATE deleted_records 
      SET processed = 1 
      WHERE source_table = ? AND destination_table = ? AND record_id = ?
    `);

    const transaction = this.db.transaction((ids) => {
      for (const id of ids) {
        stmt.run(sourceTable, destinationTable, id);
      }
    });

    transaction(recordIds);
  }

  /**
   * Log ETL run to history
   */
  logETLRun(runData) {
    const stmt = this.db.prepare(`
      INSERT INTO etl_history (
        source_table, destination_table, mode, rows_processed, rows_inserted, 
        rows_deleted, status, error_message, start_time, end_time, duration_seconds
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    stmt.run(
      runData.sourceTable,
      runData.destinationTable,
      runData.mode,
      runData.rowsProcessed,
      runData.rowsInserted,
      runData.rowsDeleted,
      runData.status,
      runData.errorMessage,
      runData.startTime,
      runData.endTime,
      runData.durationSeconds
    );
  }

  /**
   * Close the database connection
   */
  close() {
    if (this.db) {
      this.db.close();
      logger.info('SQLite database closed');
      this.db = null;
    }
  }
}

// Singleton instance
const sqliteManager = new SQLiteManager();
module.exports = sqliteManager;
