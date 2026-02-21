import sqlite3
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

def read_prices(conn: sqlite3.Connection, symbol: str) -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT symbol, date, close, volume FROM prices WHERE symbol = ? ORDER BY date",
        conn,
        params=(symbol,),
    )

def upsert_metrics(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    rows = df.copy()
    rows = rows[["symbol", "date", "return_1d", "vol_30d_ann", "drawdown", "var_95"]]
    conn.executemany(
        """
        INSERT OR REPLACE INTO derived_metrics(symbol, date, return_1d, vol_30d_ann, drawdown, var_95)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows.itertuples(index=False, name=None),
    )
    conn.commit()
    return len(rows)

def upsert_market_stress(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    rows = df.copy()
    rows = rows[["date", "slope_10y_2y", "inversion_flag"]]
    conn.executemany(
        """
        INSERT OR REPLACE INTO market_stress(date, slope_10y_2y, inversion_flag)
        VALUES (?, ?, ?)
        """,
        rows.itertuples(index=False, name=None),
    )
    conn.commit()
    return len(rows)