"""The project's rules and tables, written in plain Python (no Spark needed).

transform.py turns these tables into Spark code. The data generator and some tests use the
plain Python functions below, so they run without starting Spark.
"""
import math

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

# Season table. Each row means: (season name, the months in that season)
SEASON_MONTHS = [
    ("Winter", [12, 1, 2]),
    ("Summer", [3, 4, 5]),
    ("Monsoon", [6, 7, 8, 9]),
]
OTHER_SEASON = "Post-monsoon"  # every month not listed above (October, November)


# ---------------------------------------------------------------------------
# Small helper functions. Each one works on a single value.
# transform.py has a Spark version of each one that works on a whole column.
# ---------------------------------------------------------------------------

def is_missing(value):
    """True if the value is None or NaN (not a number)."""
    return value is None or (isinstance(value, float) and math.isnan(value))


def pm25_to_aqi(pm25):
    """Turn a PM2.5 number into an AQI score.

    We find which row of PM25_BREAKPOINTS the number falls in, then calculate
    the position inside that row. Example: PM2.5 = 45 is in the row 30-60,
    which maps to AQI 50-100, so the answer is 75.
    """
    if is_missing(pm25) or pm25 < 0:
        return None

    for pm_low, pm_high, aqi_low, aqi_high in PM25_BREAKPOINTS:
        if pm25 <= pm_high:
            steps_per_unit = (aqi_high - aqi_low) / (pm_high - pm_low)
            aqi = steps_per_unit * (pm25 - pm_low) + aqi_low
            return round(aqi, 1)

    # PM2.5 above the last row is the worst possible score
    return 500.0


def aqi_to_bucket(aqi):
    """Turn an AQI score into a label such as 'Good' or 'Poor'."""
    if is_missing(aqi):
        return None

    for highest_aqi, label in AQI_BUCKETS:
        if aqi <= highest_aqi:
            return label

    return "Severe"


def month_to_season(month):
    """Turn a month number (1-12) into a season name."""
    for season, months in SEASON_MONTHS:
        if month in months:
            return season
    return OTHER_SEASON
