from pathlib import Path

FILES = {
    "sql/schema.sql": r"""PRAGMA foreign_keys = ON;

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
""",
    "src/config.py": r"""from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    alphavantage_key: str
    fred_key: str
    db_path: str = "data.sqlite"

def get_settings() -> Settings:
    av = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()
    fred = os.getenv("FRED_API_KEY", "").strip()
    if not av:
        raise RuntimeError("Missing ALPHAVANTAGE_API_KEY in .env")
    if not fred:
        raise RuntimeError("Missing FRED_API_KEY in .env")
    return Settings(alphavantage_key=av, fred_key=fred)
""",
    "src/validation.py": r"""import pandas as pd

def require_columns(df: pd.DataFrame, cols: list[str], name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name}: missing columns {missing}")

def validate_no_duplicates(df: pd.DataFrame, keys: list[str], name: str) -> None:
    if df.duplicated(subset=keys).any():
        dups = df[df.duplicated(subset=keys, keep=False)].head(10)
        raise ValueError(f"{name}: duplicate keys found. Sample:\\n{dups}")

def validate_sorted_dates(df: pd.DataFrame, date_col: str, name: str) -> None:
    parsed = pd.to_datetime(df[date_col], errors="coerce")
    if parsed.isna().any():
        bad = df.loc[parsed.isna(), date_col].head(10).tolist()
        raise ValueError(f"{name}: unparseable dates: {bad}")
    if not parsed.is_monotonic_increasing:
        raise ValueError(f"{name}: dates not sorted ascending")

def validate_latest_nonnull(df: pd.DataFrame, date_col: str, required_cols: list[str], name: str) -> None:
    latest = df[date_col].max()
    last_rows = df[df[date_col] == latest]
    for c in required_cols:
        if last_rows[c].isna().any():
            raise ValueError(f"{name}: latest date {latest} has nulls in {c}")
""",
    "src/ingestion.py": r"""import requests
import pandas as pd

def fetch_alpha_vantage_daily(symbol: str, api_key: str) -> pd.DataFrame:
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key,
        "outputsize": "compact",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    ts = data.get("Time Series (Daily)")
    if not ts:
        raise RuntimeError(f"Alpha Vantage error for {symbol}: {data}")

    rows = []
    for date_str, values in ts.items():
        rows.append({
            "symbol": symbol,
            "date": date_str,
            "close": float(values["4. close"]),
            "volume": float(values["5. volume"]),
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    return df.sort_values("date").reset_index(drop=True)

def fetch_fred_series(series_id: str, api_key: str) -> pd.DataFrame:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {"series_id": series_id, "api_key": api_key, "file_type": "json"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    rows = []
    for o in data.get("observations", []):
        val = o.get("value")
        if val in (None, "."):
            continue
        rows.append({"series_id": series_id, "date": o["date"], "value": float(val)})

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    return df.sort_values("date").reset_index(drop=True)
""",
    "src/db.py": r"""import sqlite3
from pathlib import Path
from datetime import datetime
import pandas as pd

SCHEMA_PATH = Path("sql/schema.sql")

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.commit()

def seed_instruments(conn: sqlite3.Connection) -> None:
    instruments = [
        ("SPY", "SPDR S&P 500 ETF Trust", "Equity ETF"),
        ("QQQ", "Invesco QQQ Trust", "Equity ETF"),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO instruments(symbol, name, asset_class) VALUES (?,?,?)",
        instruments
    )
    conn.commit()

def upsert_prices(conn: sqlite3.Connection, df: pd.DataFrame, source: str) -> int:
    rows = df.copy()
    rows["source"] = source
    rows = rows[["symbol", "date", "close", "volume", "source"]]
    conn.executemany(
        "INSERT OR REPLACE INTO prices(symbol, date, close, volume, source) VALUES (?,?,?,?,?)",
        rows.itertuples(index=False, name=None)
    )
    conn.commit()
    return len(rows)

def upsert_macro(conn: sqlite3.Connection, df: pd.DataFrame, source: str) -> int:
    rows = df.copy()
    rows["source"] = source
    rows = rows[["series_id", "date", "value", "source"]]
    conn.executemany(
        "INSERT OR REPLACE INTO macro_rates(series_id, date, value, source) VALUES (?,?,?,?)",
        rows.itertuples(index=False, name=None)
    )
    conn.commit()
    return len(rows)

def log_run(conn: sqlite3.Connection, status: str, notes: str = "", rows_prices: int = 0, rows_macro: int = 0) -> None:
    conn.execute(
        "INSERT INTO pipeline_runs(run_ts, status, notes, rows_prices, rows_macro) VALUES (?,?,?,?,?)",
        (datetime.utcnow().isoformat(timespec="seconds"), status, notes, rows_prices, rows_macro),
    )
    conn.commit()
""",
    "src/run_daily.py": r"""from pathlib import Path
from src.config import get_settings
from src.db import connect, init_db, seed_instruments, upsert_prices, upsert_macro, log_run
from src.ingestion import fetch_alpha_vantage_daily, fetch_fred_series
from src.validation import (
    require_columns,
    validate_no_duplicates,
    validate_sorted_dates,
    validate_latest_nonnull,
)

def main() -> None:
    s = get_settings()
    conn = connect(s.db_path)
    init_db(conn)
    seed_instruments(conn)

    # 1) Fetch
    spy = fetch_alpha_vantage_daily("SPY", s.alphavantage_key)
    dgs2 = fetch_fred_series("DGS2", s.fred_key)
    dgs10 = fetch_fred_series("DGS10", s.fred_key)

    # 2) Validate
    require_columns(spy, ["symbol", "date", "close", "volume"], "SPY prices")
    validate_no_duplicates(spy, ["symbol", "date"], "SPY prices")
    validate_sorted_dates(spy, "date", "SPY prices")
    validate_latest_nonnull(spy, "date", ["close"], "SPY prices")

    require_columns(dgs2, ["series_id", "date", "value"], "DGS2 macro")
    validate_no_duplicates(dgs2, ["series_id", "date"], "DGS2 macro")
    validate_sorted_dates(dgs2, "date", "DGS2 macro")

    require_columns(dgs10, ["series_id", "date", "value"], "DGS10 macro")
    validate_no_duplicates(dgs10, ["series_id", "date"], "DGS10 macro")
    validate_sorted_dates(dgs10, "date", "DGS10 macro")

    # 3) Store
    rows_prices = upsert_prices(conn, spy, "AlphaVantage")
    rows_macro = upsert_macro(conn, dgs2, "FRED") + upsert_macro(conn, dgs10, "FRED")

    # 4) Output summary
    Path("outputs").mkdir(exist_ok=True)
    latest_price_date = spy["date"].max()
    latest_rates_date = min(dgs2["date"].max(), dgs10["date"].max())

    summary = (
        f"# Daily Run Summary\\n\\n"
        f"- Loaded prices rows: {rows_prices}\\n"
        f"- Loaded macro rows: {rows_macro}\\n"
        f"- Latest SPY date: {latest_price_date}\\n"
        f"- Latest rates date (overlap): {latest_rates_date}\\n"
        f"\\nStatus: SUCCESS\\n"
    )
    Path("outputs/run_summary.md").write_text(summary, encoding="utf-8")

    log_run(conn, "SUCCESS", "Loaded SPY + DGS2 + DGS10", rows_prices, rows_macro)
    conn.close()
    print("SUCCESS ✅  Wrote outputs/run_summary.md and updated data.sqlite")

if __name__ == "__main__":
    main()
""",
}

def main() -> None:
    for path, content in FILES.items():
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    print("Bootstrap complete ✅  Wrote src/*.py and sql/schema.sql")

if __name__ == "__main__":
    main()