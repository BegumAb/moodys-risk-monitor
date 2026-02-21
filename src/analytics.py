import numpy as np
import pandas as pd

def compute_metrics(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Input: prices columns = symbol, date, close
    Output: symbol, date, return_1d, vol_30d_ann, drawdown, var_95
    """
    df = prices.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["symbol", "date"])

    # 1D returns
    df["return_1d"] = df.groupby("symbol")["close"].pct_change()

    # Rolling 30D volatility (annualized)
    df["vol_30d_ann"] = (
        df.groupby("symbol")["return_1d"]
          .rolling(30, min_periods=20)
          .std()
          .reset_index(level=0, drop=True)
          * np.sqrt(252)
    )

    # Drawdown from running peak
    df["running_peak"] = df.groupby("symbol")["close"].cummax()
    df["drawdown"] = df["close"] / df["running_peak"] - 1.0

    # Rolling historical VaR 95% (1Y window)
    df["var_95"] = (
        df.groupby("symbol")["return_1d"]
          .rolling(252, min_periods=60)
          .quantile(0.05)
          .reset_index(level=0, drop=True)
    )

    out = df[["symbol", "date", "return_1d", "vol_30d_ann", "drawdown", "var_95"]].copy()
    out["date"] = out["date"].dt.date.astype(str)
    return out