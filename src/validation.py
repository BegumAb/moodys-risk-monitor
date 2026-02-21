import pandas as pd

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
