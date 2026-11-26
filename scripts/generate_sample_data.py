"""Create a realistic but SYNTHETIC (made-up) raw air-quality file with deliberate data problems.

  python scripts/generate_sample_data.py

This script uses only plain Python (no extra packages, no Spark). It writes the raw file that the Spark
pipeline then cleans.

The columns match the public 'city_day' layout (City, Date, PM2.5, ...), so you can replace
data/raw/air_quality_raw.csv with a real CPCB/Kaggle file that has the same columns.

Note: the file already in data/raw/ was made by an earlier version of this script. Running the
script again gives a similar file with the same kinds of problems, but not the same rows.
"""
import csv
import datetime
import os
import random
import sys

# Tell Python where to find our code (the src folder)
project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_folder, "src"))

# This import must stay below the line above, otherwise Python cannot find "pipeline"
from pipeline.rules import aqi_to_bucket, pm25_to_aqi

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

# The columns of the raw file, in order
RAW_COLUMNS = ["City", "Date"] + POLLUTANT_COLUMNS + ["AQI", "AQI_Bucket"]


def pick_random_rows(rng, total_rows, fraction):
    """Pick a random set of row numbers. fraction=0.03 means about 3% of the rows."""
    how_many = int(total_rows * fraction)
    return rng.sample(range(total_rows), how_many)


def list_dates(start, end):
    """Return every date from start to end (both included) as a list."""
    first_day = datetime.date.fromisoformat(start)
    last_day = datetime.date.fromisoformat(end)

    dates = []
    day = first_day
    while day <= last_day:
        dates.append(day)
        day = day + datetime.timedelta(days=1)
    return dates


def build(seed=42, start="2022-01-01", end="2023-12-31"):
    """Build the messy raw table as a list of rows (each row is a dictionary).

    The same seed always gives the same table.
    """
    rng = random.Random(seed)
    dates = list_dates(start, end)

    # ----- Part 1: make clean, believable data -----
    rows = []
    for city, (base, winter_boost) in CITY_BASE.items():
        for date in dates:
            factor = MONTH_FACTOR[date.month]
            if date.month in (12, 1, 2):
                factor = factor * winter_boost

            pm25 = base * factor * rng.lognormvariate(0, 0.3)
            pm10 = pm25 * 1.7 * rng.lognormvariate(0, 0.1)
            no2 = round(20 + 0.15 * pm25 + rng.gauss(0, 4), 2)
            so2 = round(abs(8 + rng.gauss(0, 2)), 2)
            co = round(abs(0.6 + 0.004 * pm25 + rng.gauss(0, 0.15)), 2)
            o3 = round(abs(30 + rng.gauss(0, 8)), 2)
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

    n = len(rows)

    # ----- Part 2: make the data dirty on purpose, like real files -----

    # Missing values (None is written as an empty cell in the CSV)
    for i in pick_random_rows(rng, n, 0.03):
        rows[i]["PM2.5"] = None
    for i in pick_random_rows(rng, n, 0.05):
        rows[i]["AQI"] = None  # the pipeline can calculate it again
    for i in pick_random_rows(rng, n, 0.10):
        rows[i]["AQI_Bucket"] = None

    # Impossible negative values
    for i in pick_random_rows(rng, n, 0.01):
        rows[i]["PM10"] = -rows[i]["PM10"]

    # Rows where every reading is empty
    for i in pick_random_rows(rng, n, 0.02):
        for column in POLLUTANT_COLUMNS + ["AQI", "AQI_Bucket"]:
            rows[i][column] = None

    # Messy city names (lower case, upper case, extra spaces, old name)
    for i in pick_random_rows(rng, n, 0.08):
        city = rows[i]["City"]
        if city == "Bengaluru":
            old_name = "Bangalore"
        else:
            old_name = city
        messy_choices = [city.lower(), city.upper(), " " + city + " ", old_name]
        rows[i]["City"] = rng.choice(messy_choices)

    # Impossible dates
    for i in pick_random_rows(rng, n, 0.005):
        rows[i]["Date"] = rng.choice(["2022-13-45", "N/A", "31-02-2023"])

    # Duplicate rows
    for i in pick_random_rows(rng, n, 0.015):
        rows.append(dict(rows[i]))

    # Shuffle the rows
    rng.shuffle(rows)
    return rows


def write_csv(rows, path):
    """Save the rows as a CSV file with a header row. None becomes an empty cell."""
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    out_path = os.path.join(project_folder, "data", "raw", "air_quality_raw.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    data = build()
    write_csv(data, out_path)
    print("Wrote " + str(len(data)) + " raw rows to " + out_path)
