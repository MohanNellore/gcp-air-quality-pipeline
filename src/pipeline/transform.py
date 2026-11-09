"""Step 2 - TRANSFORM: clean the data and add useful new columns, using pandas.

The main function is clean(). It fixes the data one rule at a time and counts
how many rows each rule changed. Those counts become the data-quality report.
"""
import numpy as np
import pandas as pd

# The six pollution columns
POLLUTANTS = ["pm25", "pm10", "no2", "so2", "co", "o3"]

# The final table: (column name, BigQuery column type).
# Keep this the same as the schema that load_gcp.py sends to BigQuery.
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

# Just the column names, in the same order as SCHEMA
FINAL_COLUMNS = []
for column_name, column_type in SCHEMA:
    FINAL_COLUMNS.append(column_name)

# Old city names and the new name we want instead (keys must be lowercase)
CITY_ALIASES = {
    "bangalore": "Bengaluru",
    "bombay": "Mumbai",
    "madras": "Chennai",
    "calcutta": "Kolkata",
}

# PM2.5 -> AQI conversion table. Each row means:
# (PM2.5 from, PM2.5 to, AQI from, AQI to)
# It is a simplified version of India's CPCB scale, made for learning.
PM25_BREAKPOINTS = [
    (0, 30, 0, 50),
    (30, 60, 50, 100),
    (60, 90, 100, 200),
    (90, 120, 200, 300),
    (120, 250, 300, 400),
    (250, 380, 400, 500),
]

# AQI label table. Each row means: (highest AQI for this label, label)
AQI_BUCKETS = [
    (50, "Good"),
    (100, "Satisfactory"),
    (200, "Moderate"),
    (300, "Poor"),
    (400, "Very Poor"),
]


# ---------------------------------------------------------------------------
# Small helper functions. Each one works on a single value.
# ---------------------------------------------------------------------------

def pm25_to_aqi(pm25):
    """Turn a PM2.5 number into an AQI score.

    We find which row of PM25_BREAKPOINTS the number falls in, then calculate
    the position inside that row. Example: PM2.5 = 45 is in the row 30-60,
    which maps to AQI 50-100, so the answer is 75.
    """
    if pd.isna(pm25) or pm25 < 0:
        return np.nan

    for pm_low, pm_high, aqi_low, aqi_high in PM25_BREAKPOINTS:
        if pm25 <= pm_high:
            steps_per_unit = (aqi_high - aqi_low) / (pm_high - pm_low)
            aqi = steps_per_unit * (pm25 - pm_low) + aqi_low
            return round(aqi, 1)

    # PM2.5 above the last row is the worst possible score
    return 500.0


def aqi_to_bucket(aqi):
    """Turn an AQI score into a label such as 'Good' or 'Poor'."""
    if pd.isna(aqi):
        return None

    for highest_aqi, label in AQI_BUCKETS:
        if aqi <= highest_aqi:
            return label

    return "Severe"


def month_to_season(month):
    """Turn a month number (1-12) into a season name."""
    if month in (12, 1, 2):
        return "Winter"
    if month in (3, 4, 5):
        return "Summer"
    if month in (6, 7, 8, 9):
        return "Monsoon"
    return "Post-monsoon"


def tidy_city_name(name):
    """Return a clean city name, or None if there is no usable name."""
    if pd.isna(name):
        return None

    name = str(name).strip()
    if name == "":
        return None

    # Old names are replaced by the new name (Bangalore -> Bengaluru).
    # Other names just get a capital first letter (delhi -> Delhi).
    if name.lower() in CITY_ALIASES:
        return CITY_ALIASES[name.lower()]
    return name.title()


# ---------------------------------------------------------------------------
# Cleaning steps. Each one takes the table, fixes one thing, and returns it.
# ---------------------------------------------------------------------------

def standardize_columns(df):
    """Make the column names tidy.

    'PM2.5' becomes 'pm25', 'AQI_Bucket' becomes 'aqi_bucket', 'Date' becomes 'reading_date'.
    """
    df = df.copy()

    new_names = []
    for name in df.columns:
        name = name.strip().lower()
        name = name.replace(".", "")
        name = name.replace(" ", "_")
        new_names.append(name)
    df.columns = new_names

    df = df.rename(columns={"date": "reading_date"})
    return df


def fix_city_names(df, report):
    """Rule 1: tidy the city names and drop the rows that have no city."""
    df["city"] = df["city"].apply(tidy_city_name)

    no_city = df["city"].isna()
    report["missing_city_dropped"] = int(no_city.sum())

    df = df[~no_city].copy()
    return df


def fix_dates(df, report):
    """Rule 2: a date must look like 2024-01-15. Rows with any other date are dropped."""
    parsed_dates = pd.to_datetime(df["reading_date"], format="%Y-%m-%d", errors="coerce")

    bad_date = parsed_dates.isna()
    report["invalid_dates_dropped"] = int(bad_date.sum())

    df = df[~bad_date].copy()
    df["reading_date"] = parsed_dates[~bad_date]
    return df


def fix_numbers(df, report):
    """Rule 3: change the text into numbers.

    A pollution value below zero is impossible, so we replace it with an
    empty value (NaN). The row itself is kept.
    """
    negatives_found = 0

    for column in POLLUTANTS + ["aqi"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

        is_negative = df[column] < 0
        negatives_found = negatives_found + int(is_negative.sum())
        df.loc[is_negative, column] = np.nan

    report["negative_values_nulled"] = negatives_found
    return df


def remove_duplicates(df, report):
    """Rule 4: keep only one reading for each city on each day."""
    df = df.sort_values(["city", "reading_date"]).reset_index(drop=True)

    is_duplicate = df.duplicated(subset=["city", "reading_date"], keep="first")
    report["duplicates_removed"] = int(is_duplicate.sum())

    df = df[~is_duplicate].copy()
    return df


def remove_empty_rows(df, report):
    """Rule 5: drop rows that have none of the six pollution values. They carry no information."""
    has_no_data = df[POLLUTANTS].isna().all(axis=1)
    report["empty_rows_dropped"] = int(has_no_data.sum())

    df = df[~has_no_data].copy()
    return df


def fill_aqi(df, report):
    """Rule 6: fill a missing AQI from PM2.5, and rebuild the AQI label.

    The new column aqi_source tells us where each AQI came from:
    'reported' (it was in the file) or 'derived_from_pm25' (we calculated it).
    """
    # Rows where AQI is empty but PM2.5 is available
    can_derive = df["aqi"].isna() & df["pm25"].notna()
    df.loc[can_derive, "aqi"] = df.loc[can_derive, "pm25"].apply(pm25_to_aqi)

    df["aqi_source"] = "reported"
    df.loc[can_derive, "aqi_source"] = "derived_from_pm25"
    df.loc[df["aqi"].isna(), "aqi_source"] = None  # no AQI at all, so no source

    df["aqi_bucket"] = df["aqi"].apply(aqi_to_bucket)

    report["aqi_derived_from_pm25"] = int(can_derive.sum())
    return df


def add_features(df):
    """Rule 7: add year, month and season columns, and tidy the formats."""
    df["reading_year"] = df["reading_date"].dt.year.astype(int)
    df["reading_month"] = df["reading_date"].dt.month.astype(int)
    df["season"] = df["reading_month"].apply(month_to_season)

    # Turn the date into text like 2024-01-15, which BigQuery reads as a DATE
    df["reading_date"] = df["reading_date"].dt.strftime("%Y-%m-%d")

    # Keep two digits after the decimal point
    for column in POLLUTANTS + ["aqi"]:
        df[column] = df[column].round(2)

    return df


# ---------------------------------------------------------------------------
# The main function
# ---------------------------------------------------------------------------

def clean(raw):
    """Clean the raw table.

    Returns two things: the clean table, and a report (a dictionary of counts).
    """
    df = standardize_columns(raw)

    # City and date are required. Without them we cannot continue.
    for column in ["city", "reading_date"]:
        if column not in df.columns:
            raise ValueError("Required column missing in raw data: " + column)

    report = {"rows_raw": int(len(df))}

    # If a pollution column is missing from the file, create it empty
    # instead of crashing.
    for column in POLLUTANTS + ["aqi", "aqi_bucket"]:
        if column not in df.columns:
            df[column] = np.nan

    df = fix_city_names(df, report)
    df = fix_dates(df, report)
    df = fix_numbers(df, report)
    df = remove_duplicates(df, report)
    df = remove_empty_rows(df, report)
    df = fill_aqi(df, report)
    df = add_features(df)

    # Keep only the final columns, in the right order
    df = df[FINAL_COLUMNS].reset_index(drop=True)

    # Finish the report
    report["rows_clean"] = int(len(df))
    report["cities"] = sorted(df["city"].unique().tolist())
    report["date_range"] = [df["reading_date"].min(), df["reading_date"].max()]

    # Count the empty values left in each column (only columns that have some)
    null_counts = {}
    for column, count in df.isna().sum().items():
        if count > 0:
            null_counts[column] = int(count)
    report["null_counts"] = null_counts

    return df, report
