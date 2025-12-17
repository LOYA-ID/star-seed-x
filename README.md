# Star Seed X

A lightweight Node.js ETL (Extract, Transform, Load) pipeline for transferring data between MariaDB servers.

## Why Star Seed X?

### High Performance with Low Resource Usage

| Feature | Benefit |
|---------|---------|
| **Seek/Keyset Pagination** | Uses `WHERE pk > last_value` instead of `OFFSET` - O(log n) vs O(n) performance |
| **Batch Processing** | Processes data in configurable chunks, never loads entire dataset into memory |
| **Checkpoint/Resume** | Survives crashes - automatically resumes from last successful batch |
| **Row-by-Row Insert** | Controlled memory footprint within transactions |
| **Incremental Sync** | After initial full load, only transfers new/changed data |

### Low Bandwidth & Small Footprint

```
Traditional Approach (OFFSET pagination):
┌─────────────────────────────────────────────────────────┐
│ SELECT * FROM table LIMIT 1000 OFFSET 0      → Fast     │
│ SELECT * FROM table LIMIT 1000 OFFSET 10000  → Slow     │
│ SELECT * FROM table LIMIT 1000 OFFSET 100000 → Very Slow│
│ SELECT * FROM table LIMIT 1000 OFFSET 1M     → Crash    │
└─────────────────────────────────────────────────────────┘
Database must scan and skip N rows each time!

Star Seed X (Seek/Keyset pagination):
┌─────────────────────────────────────────────────────────┐
│ SELECT * FROM table WHERE id > 0 LIMIT 1000      → Fast │
│ SELECT * FROM table WHERE id > 10000 LIMIT 1000  → Fast │
│ SELECT * FROM table WHERE id > 100000 LIMIT 1000 → Fast │
│ SELECT * FROM table WHERE id > 1000000 LIMIT 1000→ Fast │
└─────────────────────────────────────────────────────────┘
Uses index seek - consistent O(log n) performance!
```

### Handles Complex JOINs with Large Data

Star Seed X fully supports complex multi-table JOIN queries:

```sql
-- Your complex query in config:
SELECT
    u.pk_id AS id,
    u.name AS user_name,
    o.id AS order_id,
    o.order_date,
    p.name AS product_name,
    oi.qty,
    oi.price
FROM users u
JOIN orders o ON o.user_id = u.id
JOIN order_items oi ON oi.order_id = o.id
JOIN products p ON p.id = oi.product_id
WHERE o.order_date >= '2025-01-01'
```

**How it works internally:**

```sql
-- Batch 1: First 1000 rows
{your_query} ORDER BY `id` ASC LIMIT 1000

-- Batch 2: Next 1000 rows (seeks directly to id > 1000)
{your_query} AND `id` > 1000 ORDER BY `id` ASC LIMIT 1000

-- Batch N: Continues from checkpoint
{your_query} AND `id` > {last_checkpoint} ORDER BY `id` ASC LIMIT 1000
```

**Key advantages for JOIN queries:**
- Query is executed in batches - database handles JOIN optimization
- Memory stays constant regardless of total result size
- Checkpoint saves progress - resume after network/server issues
- Works with any valid SELECT including aggregations and subqueries

### Memory Comparison

| Scenario | Traditional ETL | Star Seed X |
|----------|-----------------|-------------|
| 1M rows, 1KB each | ~1GB RAM | ~1MB RAM (1000 batch) |
| 10M rows | ~10GB RAM or OOM | ~1MB RAM |
| Network interruption | Start over | Resume from checkpoint |
| Complex JOINs | Load all → process | Stream in batches |

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
- **Multi-Job Support**: Run multiple ETL jobs in parallel with separate configurations

## Installation

```bash
npm install
```

## Multi-Job Configuration

Star Seed X supports running multiple ETL jobs in parallel, each with its own configuration file.

### Creating a Job

1. Create a job configuration file in the `config/` directory:

```bash
# Copy the example template
cp config/job.example.json config/myjob.json

# Edit with your settings
nano config/myjob.json
```

2. Each job config file should contain:

```json
{
  "source": {
    "host": "source-server",
    "port": 3306,
    "user": "user",
    "password": "password",
    "database": "source_db",
    "table": "source_table"
  },
  "destination": {
    "host": "dest-server",
    "port": 3306,
    "user": "user",
    "password": "password",
    "database": "dest_db",
    "table": "dest_table"
  },
  "etl": {
    "batchSize": 1000,
    "primaryKeyColumn": "id",
    "cronSchedule": "*/5 * * * *"
  }
}
```

### Running Jobs

**Single Job (Manual/Development):**
```bash
# Run a specific job
npm run start -- --job myjob

# Or using node directly
node src/index.js --job myjob
```

**All Jobs with PM2 (Production):**
```bash
# Auto-discover and start all jobs
npm run pm2:start

# List running jobs
npm run pm2:list

# View logs for all jobs
npm run pm2:logs

# View logs for specific job
npm run pm2:logs:job star-seed-x-myjob

# Stop all jobs
npm run pm2:stop

# Restart all jobs
npm run pm2:restart
```

### Job Isolation

Each job is completely isolated with:
- **Own SQLite database**: `data/{jobname}_state.db`
- **Own log files**: `logs/{jobname}/etl.log`
- **Own PM2 process**: `star-seed-x-{jobname}`

### List Available Jobs

```bash
npm run jobs:list
```

### Clear Job Checkpoint

```bash
# Clear checkpoint for specific job
npm run etl:clear-checkpoint -- --job myjob

# Clear all checkpoints for a job
npm run etl:clear-checkpoint -- --job myjob --all
```

## Job Configuration Reference

Each job config file (`config/{jobname}.json`) supports these parameters:

### Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `source.host` | Source MariaDB server hostname | required |
| `source.port` | Source MariaDB server port | 3306 |
| `source.user` | Source database username | required |
| `source.password` | Source database password | required |
| `source.database` | Source database name | required |
| `source.table` | Source table name | required |
| `source.connectionPoolSize` | Connection pool size | 5 |
| `source.queryTimeout` | Query timeout in ms | 300000 |
| `source.maxRetries` | Max retry attempts | 3 |
| `source.retryDelay` | Delay between retries in ms | 1000 |
| `destination.*` | Same as source, for destination server | |
| `etl.batchSize` | Number of rows per batch | 1000 |
| `etl.primaryKeyColumn` | Primary key column name | "id" |
| `etl.deletedFlagColumn` | Column for soft-delete flag | "is_deleted" |
| `etl.sqlQuery` | SQL query template | "SELECT * FROM {{table}}" |
| `etl.cronSchedule` | Cron expression for scheduled runs | "*/5 * * * *" |
| `etl.maxRetries` | ETL max retry attempts | 3 |
| `etl.retryDelay` | ETL retry delay in ms | 1000 |
| `etl.forceFullRefresh` | Force full refresh on each run | false |
| `etl.recordDelay` | Delay between records in ms | 0 |
| `logging.level` | Log level (error, warn, info, debug) | "info" |

**Note:** `sqlite.dbPath` and `logging.logDir` are auto-generated based on job name if not specified.

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

## Usage

### Development Mode

```bash
npm run dev -- --job myjob
```

### Production Mode

```bash
npm run start -- --job myjob
```

### With PM2 (All Jobs)

```bash
# Start all jobs
npm run pm2:start

# Stop all jobs
npm run pm2:stop

# Restart all jobs
npm run pm2:restart

# View logs
npm run pm2:logs

# List running jobs
npm run pm2:list
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
| Source column missing in destination | **ERROR** | Fails validation - cannot insert data |
| Different data types | **WARNING** | Logs warning but continues |
| Source allows NULL, destination doesn't | **WARNING** | May cause insert failures at runtime |
| Extra columns in destination | **WARNING** | OK - destination can have additional columns |

### Schema Requirements

```
Source Table                  Destination Table
┌─────────────────────┐      ┌─────────────────────┐
│ id (INT)            │  →   │ id (INT)            │  Must exist
│ name (VARCHAR)      │  →   │ name (VARCHAR)      │  Must exist  
│ email (VARCHAR)     │  →   │ email (VARCHAR)     │  Must exist
└─────────────────────┘      │ created_at (DATE)   │  OK (extra column)
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
