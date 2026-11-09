"""Create a realistic but SYNTHETIC (made-up) raw air-quality file with deliberate data problems.

  python scripts/generate_sample_data.py

The columns match the public 'city_day' layout (City, Date, PM2.5, ...), so you can replace
data/raw/air_quality_raw.csv with a real CPCB/Kaggle file that has the same columns.
"""
import os
import sys

import numpy as np
import pandas as pd

# Tell Python where to find our code (the src folder)
project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_folder, "src"))

# This import must stay below the line above, otherwise Python cannot find "pipeline"
from pipeline.transform import aqi_to_bucket, pm25_to_aqi

# For each city: (normal PM2.5 level, extra pollution multiplier in winter)
CITY_BASE = {
    "Delhi": (110, 1.7),
    "Kolkata": (70, 1.5),
    "Mumbai": (45, 1.15),
    "Hyderabad": (40, 1.15),
    "Chennai": (35, 1.1),
    "Bengaluru": (30, 1.1),
}

# How polluted each month is, compared with a normal month
MONTH_FACTOR = {
    12: 1.0, 1: 1.0, 2: 0.9, 3: 0.75, 4: 0.7, 5: 0.7,
    6: 0.5, 7: 0.4, 8: 0.4, 9: 0.5, 10: 0.8, 11: 1.0,
}

POLLUTANT_COLUMNS = ["PM2.5", "PM10", "NO2", "SO2", "CO", "O3"]


def pick_random_rows(rng, total_rows, fraction):
    """Pick a random set of row numbers. fraction=0.03 means about 3% of the rows."""
    how_many = int(total_rows * fraction)
    return rng.choice(total_rows, size=how_many, replace=False)


def build(seed=42, start="2022-01-01", end="2023-12-31"):
    """Build the messy raw table. The same seed always gives the same table."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range(start, end)

    # ----- Part 1: make clean, believable data -----
    rows = []
    for city, (base, winter_boost) in CITY_BASE.items():
        for date in dates:
            factor = MONTH_FACTOR[date.month]
            if date.month in (12, 1, 2):
                factor = factor * winter_boost

            pm25 = base * factor * rng.lognormal(0, 0.3)
            pm10 = pm25 * 1.7 * rng.lognormal(0, 0.1)
            no2 = round(20 + 0.15 * pm25 + rng.normal(0, 4), 2)
            so2 = round(abs(8 + rng.normal(0, 2)), 2)
            co = round(abs(0.6 + 0.004 * pm25 + rng.normal(0, 0.15)), 2)
            o3 = round(abs(30 + rng.normal(0, 8)), 2)
            aqi = pm25_to_aqi(pm25)

            row = {
                "City": city,
                "Date": date.strftime("%Y-%m-%d"),
                "PM2.5": round(pm25, 2),
                "PM10": round(pm10, 2),
                "NO2": no2,
                "SO2": so2,
                "CO": co,
                "O3": o3,
                "AQI": aqi,
                "AQI_Bucket": aqi_to_bucket(aqi),
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    n = len(df)

    # ----- Part 2: make the data dirty on purpose, like real files -----

    # Missing values
    df.loc[pick_random_rows(rng, n, 0.03), "PM2.5"] = np.nan
    df.loc[pick_random_rows(rng, n, 0.05), "AQI"] = np.nan          # the pipeline can calculate it again
    df.loc[pick_random_rows(rng, n, 0.10), "AQI_Bucket"] = np.nan

    # Impossible negative values
    negative_rows = pick_random_rows(rng, n, 0.01)
    df.loc[negative_rows, "PM10"] = -df.loc[negative_rows, "PM10"]

    # Rows where every reading is empty
    empty_rows = pick_random_rows(rng, n, 0.02)
    df.loc[empty_rows, POLLUTANT_COLUMNS + ["AQI", "AQI_Bucket"]] = np.nan

    # Messy city names (lower case, upper case, extra spaces, old name)
    df["City"] = df["City"].astype(object)
    for i in pick_random_rows(rng, n, 0.08):
        city = df.at[i, "City"]
        if city == "Bengaluru":
            old_name = "Bangalore"
        else:
            old_name = city
        messy_choices = [city.lower(), city.upper(), " " + city + " ", old_name]
        df.at[i, "City"] = rng.choice(messy_choices)

    # Impossible dates
    df = df.astype({"Date": object})
    for i in pick_random_rows(rng, n, 0.005):
        df.at[i, "Date"] = rng.choice(["2022-13-45", "N/A", "31-02-2023"])

    # Duplicate rows
    duplicate_rows = df.iloc[pick_random_rows(rng, n, 0.015)]
    df = pd.concat([df, duplicate_rows], ignore_index=True)

    # Shuffle the rows
    df = df.sample(frac=1, random_state=seed).reset_index(drop=True)
    return df


if __name__ == "__main__":
    out_path = os.path.join(project_folder, "data", "raw", "air_quality_raw.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    data = build()
    data.to_csv(out_path, index=False)
    print("Wrote " + str(len(data)) + " raw rows to " + out_path)
