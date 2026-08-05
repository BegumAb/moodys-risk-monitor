import io
import requests
import pandas as pd
import yfinance as yf

def fetch_market_daily(symbol: str, api_key: str) -> pd.DataFrame:
    """
    Download full daily market history using yfinance.

    Returns columns:
    symbol, date, close, volume
    """
    df = yf.download(
        symbol,
        period="max",
        interval="1d",
        auto_adjust=False,
        progress=False,
    )

    if df.empty:
        raise RuntimeError(f"No market data returned for {symbol}")

    # yfinance may return MultiIndex columns.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()

    df = df.rename(
        columns={
            "Date": "date",
            "Close": "close",
            "Volume": "volume",
        }
    )

    df["symbol"] = symbol
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    df = df[["symbol", "date", "close", "volume"]]
    df = df.dropna(subset=["close"])

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