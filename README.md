# Market Risk Monitoring Pipeline

Production-style Python risk monitoring pipeline designed to track:

- Daily returns
- Rolling volatility (30D annualized)
- Drawdowns from peak
- 95% Historical VaR
- Yield curve stress (10Y–2Y)
- Macro inversion signals

## Overview

This project simulates a lightweight institutional-style risk monitoring system.

It:

1. Ingests market and macro data
2. Stores data in SQLite
3. Computes derived risk metrics
4. Flags abnormal risk conditions
5. Generates a daily risk watchlist

## Architecture

- `src/` → core pipeline modules
- `sql/` → schema definitions
- `bootstrap.py` → initialize database
- `run_daily.py` → daily risk execution
- `01_daily_risk_report.ipynb` → analytics & visualization

## Example Metrics

- 30D Annualized Volatility
- Max Drawdown
- Historical VaR (95%)
- Yield Curve Stress Index (Z-score)

## Tech Stack

- Python
- Pandas
- Matplotlib
- SQLite
- Git

## Purpose

Built as a portfolio project demonstrating:

- Financial risk analytics
- Data engineering structure
- Clean modular Python design
- Reproducible analytics workflow