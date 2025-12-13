# Star Seed X

A lightweight Node.js ETL (Extract, Transform, Load) pipeline for transferring data between MariaDB servers.

## Features

- **Three ETL Modes:**
  - **Full Load**: Transfers all data from source to destination (used when destination is empty)
  - **Incremental Load**: Transfers only new records based on primary key
  - **Delta Load**: Handles deleted records (removes from destination)

- **Automatic Mode Detection**: Intelligently determines which mode to use based on table state
- **Connection Pooling**: Efficient database connections using MariaDB pools
- **State Management**: SQLite-based tracking of processed records
- **Scheduled Execution**: Cron-based scheduling for automated runs
- **Comprehensive Logging**: Winston-based logging with file rotation
- **PM2 Support**: Production-ready with PM2 process manager
- **Schema Validation**: Validates compatibility between source and destination tables

## Installation

```bash
npm install
```

## Configuration

The application uses the [node-config](https://www.npmjs.com/package/config) package for configuration management.

### Configuration Files

| File | Purpose |
|------|---------|
| `config/default.json` | Base configuration with default values |
| `config/custom-environment-variables.json` | Maps environment variables to config keys |

### How Configuration Works

1. **default.json** - Contains actual default values used when the app runs
2. **custom-environment-variables.json** - Defines which environment variables can override defaults

**Priority (highest to lowest):**
1. Environment variables (if defined)
2. default.json values

### Example

```bash
# Uses default.json value "localhost"
npm start

# Environment variable overrides default
SOURCE_DB_HOST=192.168.1.100 npm start
```

### Default Configuration

Edit `config/default.json` to configure your ETL pipeline:

```json
{
  "source": {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "source_db",
    "table": "source_table",
    "connectionPoolSize": 5
  },
  "destination": {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "destination_db",
    "table": "destination_table",
    "connectionPoolSize": 5
  },
  "etl": {
    "batchSize": 1000,
    "primaryKeyColumn": "id",
    "deletedFlagColumn": "is_deleted",
    "sqlQuery": "SELECT * FROM {{table}}",
    "cronSchedule": "*/5 * * * *"
  },
  "sqlite": {
    "dbPath": "./data/etl_state.db"
  },
  "logging": {
    "level": "info",
    "logDir": "./logs"
  }
}
```

### Configuration Parameters

| Parameter | Description |
|-----------|-------------|
| `source.host` | Source MariaDB server hostname |
| `source.port` | Source MariaDB server port |
| `source.user` | Source database username |
| `source.password` | Source database password |
| `source.database` | Source database name |
| `source.table` | Source table name |
| `source.connectionPoolSize` | Connection pool size for source |
| `destination.*` | Same as source, for destination server |
| `etl.batchSize` | Number of rows to read per batch |
| `etl.primaryKeyColumn` | Primary key column name |
| `etl.deletedFlagColumn` | Column name for soft-delete flag |
| `etl.sqlQuery` | SQL query template (use `{{table}}` as placeholder) |
| `etl.cronSchedule` | Cron expression for scheduled runs |
| `sqlite.dbPath` | Path to SQLite state database |
| `logging.level` | Log level (error, warn, info, debug) |
| `logging.logDir` | Directory for log files |

### SQL Query Configuration

The `etl.sqlQuery` parameter supports custom SQL queries including JOINs and column aliases.

#### Basic Usage
```json
"sqlQuery": "SELECT * FROM {{table}}"
```

#### With JOINs
```json
"sqlQuery": "SELECT a.id, a.name, b.category_name FROM {{table}} a JOIN categories b ON a.category_id = b.id"
```

#### With Specific Columns
```json
"sqlQuery": "SELECT id, name, email, created_at FROM {{table}}"
```

**Important Notes:**
- Use `{{table}}` as placeholder for the source table name
- The application validates SQL syntax before execution using `EXPLAIN`
- Column detection is automatic from the query result (not just table schema)
- All columns returned by your query must exist in the destination table
- For incremental load, ensure the primary key column is included in your SELECT

### Environment Variables

You can override any configuration using environment variables. The mapping is defined in `config/custom-environment-variables.json`:

| Environment Variable | Config Path |
|---------------------|-------------|
| `SOURCE_DB_HOST` | source.host |
| `SOURCE_DB_PORT` | source.port |
| `SOURCE_DB_USER` | source.user |
| `SOURCE_DB_PASSWORD` | source.password |
| `SOURCE_DB_NAME` | source.database |
| `SOURCE_TABLE_NAME` | source.table |
| `SOURCE_POOL_SIZE` | source.connectionPoolSize |
| `DEST_DB_HOST` | destination.host |
| `DEST_DB_PORT` | destination.port |
| `DEST_DB_USER` | destination.user |
| `DEST_DB_PASSWORD` | destination.password |
| `DEST_DB_NAME` | destination.database |
| `DEST_TABLE_NAME` | destination.table |
| `DEST_POOL_SIZE` | destination.connectionPoolSize |
| `ETL_BATCH_SIZE` | etl.batchSize |
| `ETL_PRIMARY_KEY_COLUMN` | etl.primaryKeyColumn |
| `ETL_DELETED_FLAG_COLUMN` | etl.deletedFlagColumn |
| `ETL_SQL_QUERY` | etl.sqlQuery |
| `ETL_CRON_SCHEDULE` | etl.cronSchedule |
| `SQLITE_DB_PATH` | sqlite.dbPath |
| `LOG_LEVEL` | logging.level |
| `LOG_DIR` | logging.logDir |

**Tip:** Use environment variables in production to avoid committing sensitive data (passwords) to version control.

## Usage

### Development Mode

```bash
npm run dev
```

### Production Mode

```bash
npm start
```

### With PM2

```bash
# Start the application
npm run pm2:start

# Stop the application
npm run pm2:stop

# Restart the application
npm run pm2:restart

# View logs
npm run pm2:logs
```

## ETL Modes

### Full Load
- Triggered when the destination table is empty
- Transfers all records from source to destination
- Data is read in batches but inserted row by row

### Incremental Load
- Triggered when destination has data and source has a primary key column
- Only transfers records with primary key greater than the last processed value
- State is tracked in SQLite database

### Delta Load
- Triggered when source table has a deleted flag column with deleted records
- Deletes corresponding records from destination table
- Processed deletions are tracked in SQLite

## Project Structure

```
star-seed-x/
├── config/
│   ├── default.json                    # Default configuration values
│   └── custom-environment-variables.json # Environment variable mappings
├── src/
│   ├── database/
│   │   ├── mariadb.js        # MariaDB pool manager
│   │   └── sqlite.js         # SQLite state manager
│   ├── etl/
│   │   ├── etlRunner.js      # Main ETL orchestrator
│   │   ├── fullLoad.js       # Full load processor
│   │   ├── incrementalLoad.js # Incremental load processor
│   │   ├── deltaLoad.js      # Delta load processor
│   │   └── modeDetector.js   # ETL mode detection
│   ├── utils/
│   │   ├── connectionChecker.js # Connection health checks
│   │   └── schemaValidator.js   # Schema validation
│   ├── config.js             # Configuration loader
│   ├── logger.js             # Winston logger setup
│   └── index.js              # Application entry point
├── ecosystem.config.js       # PM2 configuration
├── package.json
└── README.md
```

## Schema Validation

Before transferring data, the application validates that source and destination table schemas are compatible.

### Validation Rules

| Check | Result | Description |
|-------|--------|-------------|
| Source column missing in destination | ❌ **ERROR** | Fails validation - cannot insert data |
| Different data types | ⚠️ **WARNING** | Logs warning but continues |
| Source allows NULL, destination doesn't | ⚠️ **WARNING** | May cause insert failures at runtime |
| Extra columns in destination | ⚠️ **WARNING** | OK - destination can have additional columns |

### Schema Requirements

```
Source Table                  Destination Table
┌─────────────────────┐      ┌─────────────────────┐
│ id (INT)            │  →   │ id (INT)            │  ✅ Must exist
│ name (VARCHAR)      │  →   │ name (VARCHAR)      │  ✅ Must exist  
│ email (VARCHAR)     │  →   │ email (VARCHAR)     │  ✅ Must exist
└─────────────────────┘      │ created_at (DATE)   │  ⚠️ OK (extra column)
                             └─────────────────────┘
```

**Key Point:** All source columns must exist in the destination table. The destination can have extra columns (they will be NULL or use default values).

### Compatible Data Types

The following data types are considered compatible with each other:

| Type Group | Compatible Types |
|------------|------------------|
| Integers | `int`, `integer`, `bigint`, `smallint`, `tinyint`, `mediumint` |
| Strings | `varchar`, `char`, `text`, `longtext`, `mediumtext`, `tinytext` |
| Decimals | `decimal`, `numeric`, `float`, `double`, `real` |
| Timestamps | `datetime`, `timestamp` |
| Binary | `blob`, `longblob`, `mediumblob`, `tinyblob`, `binary`, `varbinary` |

For example, transferring from `INT` to `BIGINT` is OK, but `VARCHAR` to `INT` will trigger a warning.

## Logging

Logs are written to:
- Console (with colors)
- `logs/etl.log` (all logs)
- `logs/error.log` (errors only)

Log files are automatically rotated when they reach 10MB.

## Requirements

- Node.js 16+
- MariaDB 10.3+
- PM2 (optional, for production deployment)

## License

MIT
