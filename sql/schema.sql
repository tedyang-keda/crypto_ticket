CREATE TABLE IF NOT EXISTS symbol_registry (
  exchange VARCHAR(16) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  market_type VARCHAR(16) NOT NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  first_seen_at_ms BIGINT NOT NULL,
  last_seen_at_ms BIGINT NOT NULL,
  last_status VARCHAR(32) NOT NULL,
  raw_json JSON NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (exchange, symbol),
  KEY idx_registry_active (exchange, is_active, last_seen_at_ms)
);

CREATE TABLE IF NOT EXISTS latest_quote (
  exchange VARCHAR(16) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  ts_ms BIGINT NOT NULL,
  price DECIMAL(28, 12) NOT NULL,
  size DECIMAL(28, 12) DEFAULT NULL,
  side VARCHAR(8) DEFAULT NULL,
  raw_json JSON NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (exchange, symbol),
  KEY idx_latest_quote_ts (exchange, ts_ms)
);

CREATE TABLE IF NOT EXISTS bar_checkpoint (
  exchange VARCHAR(16) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  timeframe VARCHAR(8) NOT NULL,
  start_ms BIGINT NOT NULL,
  end_ms BIGINT NOT NULL,
  open_price DECIMAL(28, 12) NOT NULL,
  high_price DECIMAL(28, 12) NOT NULL,
  low_price DECIMAL(28, 12) NOT NULL,
  close_price DECIMAL(28, 12) NOT NULL,
  volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  quote_volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  trade_count BIGINT NOT NULL DEFAULT 0,
  last_tick_ms BIGINT NOT NULL,
  is_final TINYINT(1) NOT NULL DEFAULT 0,
  raw_json JSON NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (exchange, symbol, timeframe),
  KEY idx_bar_checkpoint_time (exchange, timeframe, start_ms)
);

CREATE TABLE IF NOT EXISTS bar_history (
  exchange VARCHAR(16) NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  timeframe VARCHAR(8) NOT NULL,
  start_ms BIGINT NOT NULL,
  end_ms BIGINT NOT NULL,
  open_price DECIMAL(28, 12) NOT NULL,
  high_price DECIMAL(28, 12) NOT NULL,
  low_price DECIMAL(28, 12) NOT NULL,
  close_price DECIMAL(28, 12) NOT NULL,
  volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  quote_volume DECIMAL(30, 12) NOT NULL DEFAULT 0,
  trade_count BIGINT NOT NULL DEFAULT 0,
  last_tick_ms BIGINT NOT NULL,
  is_final TINYINT(1) NOT NULL DEFAULT 0,
  raw_json JSON NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (exchange, symbol, timeframe, start_ms)
);

CREATE TABLE IF NOT EXISTS archive_manifest (
  exchange VARCHAR(16) NOT NULL,
  timeframe VARCHAR(8) NOT NULL,
  partition_key VARCHAR(32) NOT NULL,
  file_path TEXT NOT NULL,
  start_ms BIGINT NOT NULL,
  end_ms BIGINT NOT NULL,
  bar_count BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (exchange, timeframe, partition_key)
);
