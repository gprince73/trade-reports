"""Load and parse pipe-delimited data feed files."""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from config import CSV_DATA_DIR, CSV_FILES, CHART_LOOKBACK_SECONDS


def _parse_dollar(val: str) -> Optional[float]:
    """Convert '$67,416.28' or '$-34.71' or 'N/A' to float or None."""
    val = val.strip()
    if val == "N/A" or not val:
        return None
    return float(val.replace("$", "").replace(",", ""))


def _parse_feeds(val: str) -> Optional[float]:
    """Convert '3+1/4' or '4/4' to float. E.g. '3+1/4' -> 3.25."""
    val = val.strip()
    if not val or val == "N/A":
        return None
    # Format: "3+1/4" or "4/4" or "2+1/4"
    m = re.match(r"(\d+)\+(\d+)/(\d+)", val)
    if m:
        return int(m.group(1)) + int(m.group(2)) / int(m.group(3))
    m = re.match(r"(\d+)/(\d+)", val)
    if m:
        return int(m.group(1)) / int(m.group(2))
    return float(val)


def load_feed(asset: str) -> pd.DataFrame:
    """Load a data_feed5_{asset}.txt file into a DataFrame.

    Returns DataFrame with columns:
        datetime, Strike, PriceProxy, GrowingCP, Gap, SD_max, Secs, Feeds
    """
    filename = CSV_FILES.get(asset.upper())
    if filename is None:
        raise ValueError(f"Unknown asset: {asset}. Known: {list(CSV_FILES.keys())}")

    filepath = CSV_DATA_DIR / filename
    if not filepath.exists():
        raise FileNotFoundError(f"Data file not found: {filepath}")

    # Read raw lines, skip comment lines (start with #) and separator line
    rows = []
    header = None
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped.startswith("#") or stripped.startswith("---") or not stripped:
                continue
            if header is None:
                header = [col.strip() for col in stripped.split("|")]
                continue
            # Data row
            cols = [col.strip() for col in stripped.split("|")]
            if len(cols) >= len(header):
                rows.append(cols[: len(header)])

    df = pd.DataFrame(rows, columns=header)

    # Parse columns
    df["datetime"] = pd.to_datetime(df["Date"] + " " + df["Time"])
    df["Strike"] = df["Strike"].apply(_parse_dollar)
    df["PriceProxy"] = df["PriceProxy"].apply(_parse_dollar)
    df["GrowingCP"] = df["GrowingCP"].apply(_parse_dollar)
    df["Gap"] = df["Gap"].apply(_parse_dollar)
    df["SD_max"] = df["SD_max"].apply(_parse_dollar)
    df["Secs"] = pd.to_numeric(df["Secs"], errors="coerce").astype("Int64")
    df["Feeds"] = df["Feeds"].apply(_parse_feeds)

    # Drop original Date/Time columns
    df = df.drop(columns=["Date", "Time"])

    return df


def get_contract_window(
    df: pd.DataFrame,
    expiry_time: datetime,
    lookback_seconds: int = CHART_LOOKBACK_SECONDS,
) -> pd.DataFrame:
    """Extract a time window of CSV data ending at contract expiry.

    Args:
        df: Full data feed DataFrame (from load_feed).
        expiry_time: The contract expiry datetime.
        lookback_seconds: How many seconds before expiry to include.

    Returns:
        Filtered DataFrame for the time window [expiry - lookback, expiry].
    """
    start = expiry_time - timedelta(seconds=lookback_seconds)
    mask = (df["datetime"] >= start) & (df["datetime"] <= expiry_time)
    return df.loc[mask].copy()


def get_settlement_strike(df: pd.DataFrame, expiry_time: datetime) -> Optional[float]:
    """Find the settlement price = Strike of the next contract after expiry.

    After a contract expires, the CSV transitions from N/A (or the old strike)
    to the new strike value. The first non-null Strike after expiry is the
    settlement price (= next contract's strike = the price the expired contract
    settled at).
    """
    # Look at rows after expiry
    after = df.loc[df["datetime"] > expiry_time].copy()
    if after.empty:
        return None

    # Find first row with a valid Strike
    valid = after.dropna(subset=["Strike"])
    if valid.empty:
        return None

    return float(valid.iloc[0]["Strike"])
