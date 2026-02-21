import io
import requests
import pandas as pd

def fetch_market_daily(symbol: str, api_key: str) -> pd.DataFrame:
    """
    Market daily data with reliability-first design:
    1) Try Alpha Vantage REST (may fail due to premium/rate limits)
    2) Fall back to Stooq free CSV over HTTP (no key)
    Returns columns: symbol, date, close, volume
    """
    # ---- Try Alpha Vantage first ----
    try:
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
            raise RuntimeError(str(data))

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

    except Exception:
        # ---- Fallback: Stooq (reliable + free) ----
        # ETFs and US stocks typically use ".us"
        stooq_symbol = symbol.lower() + ".us"
        stooq_url = f"https://stooq.com/q/d/l/?s={stooq_symbol}&i=d"
        resp = requests.get(stooq_url, timeout=30)
        resp.raise_for_status()

        df = pd.read_csv(io.StringIO(resp.text))
        # Stooq columns: Date, Open, High, Low, Close, Volume
        df = df.rename(columns={"Date": "date", "Close": "close", "Volume": "volume"})
        df["symbol"] = symbol
        df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["volume"] = pd.to_numeric(df.get("volume", None), errors="coerce")
        df = df[["symbol", "date", "close", "volume"]].dropna(subset=["close"])
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