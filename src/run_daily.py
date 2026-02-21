from pathlib import Path
import pandas as pd

from src.config import get_settings
from src.db import (
    connect,
    init_db,
    seed_instruments,
    upsert_prices,
    upsert_macro,
    upsert_metrics,
    upsert_market_stress,
    read_prices,
    log_run,
)
from src.ingestion import fetch_market_daily, fetch_fred_series
from src.validation import (
    require_columns,
    validate_no_duplicates,
    validate_sorted_dates,
    validate_latest_nonnull,
)
from src.analytics import compute_metrics


def build_watchlist(metrics: pd.DataFrame) -> pd.DataFrame:
    """
    Simple Daily Risk Watchlist:
    Flag if vol_30d_ann is high, drawdown is deep, or today's return breaches VaR95.
    """
    latest_date = metrics["date"].max()
    latest = metrics[metrics["date"] == latest_date].copy()

    latest["flag_vol"] = latest["vol_30d_ann"] > 0.15
    latest["flag_dd"] = latest["drawdown"] < -0.03
    latest["flag_var"] = latest["return_1d"] < latest["var_95"] # VaR breach

    latest["flags_count"] = latest[["flag_vol", "flag_dd", "flag_var"]].sum(axis=1)

    cols = [
        "symbol",
        "date",
        "return_1d",
        "vol_30d_ann",
        "drawdown",
        "var_95",
        "flag_vol",
        "flag_dd",
        "flag_var",
        "flags_count",
    ]
    return latest[cols].sort_values(["flags_count", "vol_30d_ann"], ascending=[False, False])


def compute_yield_curve_slope(dgs2: pd.DataFrame, dgs10: pd.DataFrame) -> pd.DataFrame:
    """
    Input:
      dgs2 columns: series_id, date, value
      dgs10 columns: series_id, date, value
    Output:
      date, slope_10y_2y, inversion_flag
    """
    two = dgs2.rename(columns={"value": "dgs2"})[["date", "dgs2"]]
    ten = dgs10.rename(columns={"value": "dgs10"})[["date", "dgs10"]]

    rates = pd.merge(ten, two, on="date", how="inner")
    rates["slope_10y_2y"] = rates["dgs10"] - rates["dgs2"]
    rates["inversion_flag"] = (rates["slope_10y_2y"] < 0).astype(int)

    out = rates[["date", "slope_10y_2y", "inversion_flag"]].copy()
    out = out.sort_values("date").reset_index(drop=True)
    return out


def main() -> None:
    s = get_settings()
    conn = connect(s.db_path)
    init_db(conn)
    seed_instruments(conn)

    # 1) Fetch market + macro
    spy = fetch_market_daily("SPY", s.alphavantage_key)
    qqq = fetch_market_daily("QQQ", s.alphavantage_key)

    dgs2 = fetch_fred_series("DGS2", s.fred_key)
    dgs10 = fetch_fred_series("DGS10", s.fred_key)

    # 2) Validate market
    require_columns(spy, ["symbol", "date", "close", "volume"], "SPY prices")
    validate_no_duplicates(spy, ["symbol", "date"], "SPY prices")
    validate_sorted_dates(spy, "date", "SPY prices")
    validate_latest_nonnull(spy, "date", ["close"], "SPY prices")

    require_columns(qqq, ["symbol", "date", "close", "volume"], "QQQ prices")
    validate_no_duplicates(qqq, ["symbol", "date"], "QQQ prices")
    validate_sorted_dates(qqq, "date", "QQQ prices")
    validate_latest_nonnull(qqq, "date", ["close"], "QQQ prices")

    # 3) Validate macro
    require_columns(dgs2, ["series_id", "date", "value"], "DGS2 macro")
    validate_no_duplicates(dgs2, ["series_id", "date"], "DGS2 macro")
    validate_sorted_dates(dgs2, "date", "DGS2 macro")

    require_columns(dgs10, ["series_id", "date", "value"], "DGS10 macro")
    validate_no_duplicates(dgs10, ["series_id", "date"], "DGS10 macro")
    validate_sorted_dates(dgs10, "date", "DGS10 macro")

    # 4) Store raw
    rows_prices = upsert_prices(conn, spy, "MarketData") + upsert_prices(conn, qqq, "MarketData")
    rows_macro = upsert_macro(conn, dgs2, "FRED") + upsert_macro(conn, dgs10, "FRED")

    # 5) Compute + store yield curve slope
    stress_df = compute_yield_curve_slope(dgs2, dgs10)
    rows_stress = upsert_market_stress(conn, stress_df)

    # 6) Analytics from DB (compute from stored data)
    prices_spy = read_prices(conn, "SPY")
    prices_qqq = read_prices(conn, "QQQ")
    prices_all = pd.concat([prices_spy, prices_qqq], ignore_index=True)

    metrics_df = compute_metrics(prices_all[["symbol", "date", "close"]])
    rows_metrics = upsert_metrics(conn, metrics_df)

    # 7) Outputs
    Path("outputs").mkdir(exist_ok=True)

    watchlist = build_watchlist(metrics_df)
    watchlist.to_csv("outputs/daily_watchlist.csv", index=False)

    stress_df.to_csv("outputs/macro_stress.csv", index=False)

    # 8) Summary
    latest_price_date = max(spy["date"].max(), qqq["date"].max())
    latest_rates_date = min(dgs2["date"].max(), dgs10["date"].max())
    latest_metrics_date = metrics_df["date"].max()

    latest_stress = stress_df.iloc[-1]
    slope = float(latest_stress["slope_10y_2y"])
    inv = int(latest_stress["inversion_flag"])

    summary = (
        f"# Daily Run Summary\n\n"
        f"- Loaded prices rows: {rows_prices}\n"
        f"- Loaded macro rows: {rows_macro}\n"
        f"- Loaded metrics rows: {rows_metrics}\n"
        f"- Loaded market stress rows: {rows_stress}\n"
        f"- Latest market date: {latest_price_date}\n"
        f"- Latest rates date (overlap): {latest_rates_date}\n"
        f"- Latest metrics date: {latest_metrics_date}\n\n"
        f"## Macro Stress\n"
        f"- 10Y-2Y slope: {slope:.3f}\n"
        f"- Inversion flag (1=inverted): {inv}\n\n"
        f"## Daily Risk Watchlist (top)\n\n"
        f"{watchlist.head(10).to_markdown(index=False)}\n\n"
        f"Status: SUCCESS\n"
    )

    Path("outputs/run_summary.md").write_text(summary, encoding="utf-8")

    log_run(conn, "SUCCESS", "Loaded SPY+QQQ, macro rates, computed metrics + yield curve slope", rows_prices, rows_macro)
    conn.close()

    print("SUCCESS ✅  Wrote outputs/run_summary.md, outputs/daily_watchlist.csv, outputs/macro_stress.csv, and updated data.sqlite")


if __name__ == "__main__":
    main()