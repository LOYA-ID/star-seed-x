module.exports = {
  apps: [
    {
      name: 'star-seed-x',
      script: 'src/index.js',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '500M',
      env: {
        NODE_ENV: 'development'
        // Development values - add your dev settings here
        // SOURCE_DB_HOST: 'localhost',
        // SOURCE_DB_PORT: 3306,
        // SOURCE_DB_USER: 'root',
        // SOURCE_DB_PASSWORD: 'dev_password',
        // SOURCE_DB_NAME: 'source_db',
        // SOURCE_TABLE_NAME: 'source_table',
        // DEST_DB_HOST: 'localhost',
        // DEST_DB_PORT: 3306,
        // DEST_DB_USER: 'root',
        // DEST_DB_PASSWORD: 'dev_password',
        // DEST_DB_NAME: 'dest_db',
        // DEST_TABLE_NAME: 'dest_table'
      },
      env_production: {
        NODE_ENV: 'production'
        // Production values - add your prod settings here
        // SOURCE_DB_HOST: 'prod-source-server',
        // SOURCE_DB_PORT: 3306,
        // SOURCE_DB_USER: 'etl_user',
        // SOURCE_DB_PASSWORD: 'prod_password',
        // SOURCE_DB_NAME: 'production_db',
        // SOURCE_TABLE_NAME: 'data_table',
        // DEST_DB_HOST: 'prod-dest-server',
        // DEST_DB_PORT: 3306,
        // DEST_DB_USER: 'etl_user',
        // DEST_DB_PASSWORD: 'prod_password',
        // DEST_DB_NAME: 'warehouse_db',
        // DEST_TABLE_NAME: 'data_table'
      },
      error_file: './logs/pm2-error.log',
      out_file: './logs/pm2-out.log',
      log_file: './logs/pm2-combined.log',
      time: true,
      // Graceful shutdown configuration
      kill_timeout: 30000, // 30 seconds to gracefully shutdown
      wait_ready: true,
      listen_timeout: 10000,
      // Restart configuration
      exp_backoff_restart_delay: 1000,
      max_restarts: 10,
      restart_delay: 1000
    }
  ]
};
