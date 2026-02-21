PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS instruments (
  symbol TEXT PRIMARY KEY,
  name TEXT,
  asset_class TEXT
);

CREATE TABLE IF NOT EXISTS prices (
  symbol TEXT NOT NULL,
  date TEXT NOT NULL,              -- YYYY-MM-DD
  close REAL NOT NULL,
  volume REAL,
  source TEXT NOT NULL,
  PRIMARY KEY (symbol, date),
  FOREIGN KEY (symbol) REFERENCES instruments(symbol)
);

CREATE TABLE IF NOT EXISTS macro_rates (
  series_id TEXT NOT NULL,         -- e.g., DGS2, DGS10
  date TEXT NOT NULL,
  value REAL NOT NULL,
  source TEXT NOT NULL,
  PRIMARY KEY (series_id, date)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_ts TEXT NOT NULL,
  status TEXT NOT NULL,
  notes TEXT,
  rows_prices INTEGER,
  rows_macro INTEGER
);
CREATE TABLE IF NOT EXISTS derived_metrics (
  symbol TEXT NOT NULL,
  date TEXT NOT NULL,
  return_1d REAL,
  vol_30d_ann REAL,
  drawdown REAL,
  var_95 REAL,
  PRIMARY KEY (symbol, date),
  FOREIGN KEY (symbol) REFERENCES instruments(symbol)
);CREATE TABLE IF NOT EXISTS market_stress (
  date TEXT PRIMARY KEY,
  slope_10y_2y REAL,
  inversion_flag INTEGER
);