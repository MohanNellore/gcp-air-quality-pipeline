"""Step 2 - TRANSFORM: clean, standardise and enrich the data with Pandas.

Every cleaning rule counts what it changed, so the pipeline can print a data-quality report.
"""
import numpy as np
import pandas as pd

POLLUTANTS = ["pm25", "pm10", "no2", "so2", "co", "o3"]

# Final table layout. Keep this in sync with the BigQuery schema in load_gcp.py
SCHEMA = [
    ("city", "STRING"),
    ("reading_date", "DATE"),
    ("pm25", "FLOAT"),
    ("pm10", "FLOAT"),
    ("no2", "FLOAT"),
    ("so2", "FLOAT"),
    ("co", "FLOAT"),
    ("o3", "FLOAT"),
    ("aqi", "FLOAT"),
    ("aqi_bucket", "STRING"),
    ("aqi_source", "STRING"),
    ("reading_year", "INTEGER"),
    ("reading_month", "INTEGER"),
    ("season", "STRING"),
]
FINAL_COLUMNS = [name for name, _ in SCHEMA]

CITY_ALIASES = {"bangalore": "Bengaluru", "bombay": "Mumbai", "madras": "Chennai", "calcutta": "Kolkata"}

# Simplified PM2.5 -> AQI breakpoints (low, high, aqi_low, aqi_high), modelled on India's CPCB scale
PM25_BREAKPOINTS = [(0, 30, 0, 50), (30, 60, 50, 100), (60, 90, 100, 200),
                    (90, 120, 200, 300), (120, 250, 300, 400), (250, 380, 400, 500)]
AQI_BUCKETS = [(50, "Good"), (100, "Satisfactory"), (200, "Moderate"),
               (300, "Poor"), (400, "Very Poor")]


def pm25_to_aqi(value: float) -> float:
    """Linear interpolation inside the matching breakpoint band."""
    if value is None or pd.isna(value) or value < 0:
        return np.nan
    if value >= 380:
        return 500.0
    for lo, hi, a_lo, a_hi in PM25_BREAKPOINTS:
        if value <= hi:
            return round((a_hi - a_lo) / (hi - lo) * (value - lo) + a_lo, 1)
    return 500.0


def aqi_to_bucket(value: float):
    if value is None or pd.isna(value):
        return None
    for upper, label in AQI_BUCKETS:
        if value <= upper:
            return label
    return "Severe"


def month_to_season(month: int) -> str:
    if month in (12, 1, 2):
        return "Winter"
    if month in (3, 4, 5):
        return "Summer"
    if month in (6, 7, 8, 9):
        return "Monsoon"
    return "Post-monsoon"


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """'PM2.5' -> 'pm25', 'AQI_Bucket' -> 'aqi_bucket', 'Date' -> 'reading_date'."""
    out = df.copy()
    out.columns = [c.strip().lower().replace(".", "").replace(" ", "_") for c in out.columns]
    return out.rename(columns={"date": "reading_date"})


def clean(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Return (clean dataframe, quality report)."""
    df = standardize_columns(raw)
    for needed in ("city", "reading_date"):
        if needed not in df.columns:
            raise ValueError(f"Required column missing in raw data: {needed}")
    report: dict = {"rows_raw": int(len(df))}

    # Optional columns that are absent become empty instead of crashing the run
    for col in POLLUTANTS + ["aqi", "aqi_bucket"]:
        if col not in df.columns:
            df[col] = np.nan

    # 1. City names: trim, fix case, map old names (Bangalore -> Bengaluru)
    city = df["city"].astype(object).where(df["city"].notna(), None)
    city = city.map(lambda x: x.strip() if isinstance(x, str) else x)
    missing_city = city.isna() | (city == "")
    report["missing_city_dropped"] = int(missing_city.sum())
    df = df.loc[~missing_city].copy()
    df["city"] = city[~missing_city].map(lambda x: CITY_ALIASES.get(x.lower(), x.title()))

    # 2. Dates: must be YYYY-MM-DD, anything else is dropped
    parsed = pd.to_datetime(df["reading_date"], format="%Y-%m-%d", errors="coerce")
    report["invalid_dates_dropped"] = int(parsed.isna().sum())
    df = df.loc[parsed.notna()].copy()
    df["reading_date"] = parsed[parsed.notna()]

    # 3. Numbers: text -> float; negative concentrations are impossible, so set them to empty
    negatives = 0
    for col in POLLUTANTS + ["aqi"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        neg = df[col] < 0
        negatives += int(neg.sum())
        df.loc[neg, col] = np.nan
    report["negative_values_nulled"] = negatives

    # 4. Duplicates: one reading per city per day
    df = df.sort_values(["city", "reading_date"]).reset_index(drop=True)
    dup = df.duplicated(subset=["city", "reading_date"], keep="first")
    report["duplicates_removed"] = int(dup.sum())
    df = df.loc[~dup].copy()

    # 5. Rows with no pollutant measurement at all carry no information
    empty = df[POLLUTANTS].isna().all(axis=1)
    report["empty_rows_dropped"] = int(empty.sum())
    df = df.loc[~empty].copy()

    # 6. Fill a missing AQI from PM2.5, remember where each AQI came from, rebuild the bucket
    derived = df["aqi"].isna() & df["pm25"].notna()
    df.loc[derived, "aqi"] = df.loc[derived, "pm25"].map(pm25_to_aqi)
    df["aqi_source"] = np.where(df["aqi"].isna(), None, np.where(derived, "derived_from_pm25", "reported"))
    df["aqi_bucket"] = df["aqi"].map(aqi_to_bucket)
    report["aqi_derived_from_pm25"] = int(derived.sum())

    # 7. Features for analysis
    df["reading_year"] = df["reading_date"].dt.year.astype(int)
    df["reading_month"] = df["reading_date"].dt.month.astype(int)
    df["season"] = df["reading_month"].map(month_to_season)
    df["reading_date"] = df["reading_date"].dt.strftime("%Y-%m-%d")
    for col in POLLUTANTS + ["aqi"]:
        df[col] = df[col].round(2)

    df = df[FINAL_COLUMNS].reset_index(drop=True)
    report["rows_clean"] = int(len(df))
    report["cities"] = sorted(df["city"].unique().tolist())
    report["date_range"] = [df["reading_date"].min(), df["reading_date"].max()]
    report["null_counts"] = {c: int(v) for c, v in df.isna().sum().items() if v}
    return df, report
