"""Create a realistic but SYNTHETIC raw air-quality file with deliberate data problems.

  python scripts/generate_sample_data.py

The columns match the public 'city_day' style layout (City, Date, PM2.5, ...), so you can replace
data/raw/air_quality_raw.csv with a real CPCB/Kaggle file that has the same columns.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from pipeline.transform import aqi_to_bucket, pm25_to_aqi  # noqa: E402

CITY_BASE = {"Delhi": (110, 1.7), "Kolkata": (70, 1.5), "Mumbai": (45, 1.15),
             "Hyderabad": (40, 1.15), "Chennai": (35, 1.1), "Bengaluru": (30, 1.1)}
MONTH_FACTOR = {12: 1.0, 1: 1.0, 2: 0.9, 3: 0.75, 4: 0.7, 5: 0.7,
                6: 0.5, 7: 0.4, 8: 0.4, 9: 0.5, 10: 0.8, 11: 1.0}


def build(seed: int = 42, start: str = "2022-01-01", end: str = "2023-12-31") -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range(start, end)
    rows = []
    for city, (base, winter_boost) in CITY_BASE.items():
        for d in dates:
            f = MONTH_FACTOR[d.month]
            if d.month in (12, 1, 2):
                f *= winter_boost
            pm25 = base * f * rng.lognormal(0, 0.3)
            pm10 = pm25 * 1.7 * rng.lognormal(0, 0.1)
            aqi = pm25_to_aqi(pm25)
            rows.append({"City": city, "Date": d.strftime("%Y-%m-%d"), "PM2.5": round(pm25, 2),
                         "PM10": round(pm10, 2), "NO2": round(20 + 0.15 * pm25 + rng.normal(0, 4), 2),
                         "SO2": round(abs(8 + rng.normal(0, 2)), 2), "CO": round(abs(0.6 + 0.004 * pm25 + rng.normal(0, 0.15)), 2),
                         "O3": round(abs(30 + rng.normal(0, 8)), 2), "AQI": aqi, "AQI_Bucket": aqi_to_bucket(aqi)})
    df = pd.DataFrame(rows)
    n = len(df)

    # ---- deliberately dirty the data, like real files ----
    pick = lambda frac: rng.choice(n, size=int(n * frac), replace=False)  # noqa: E731
    df.loc[pick(0.03), "PM2.5"] = np.nan                       # missing PM2.5
    df.loc[pick(0.05), "AQI"] = np.nan                         # missing AQI (pipeline can derive it)
    df.loc[pick(0.10), "AQI_Bucket"] = np.nan                  # missing bucket
    neg = pick(0.01)
    df.loc[neg, "PM10"] = -df.loc[neg, "PM10"]                 # impossible negative values
    empty = pick(0.02)
    df.loc[empty, ["PM2.5", "PM10", "NO2", "SO2", "CO", "O3", "AQI", "AQI_Bucket"]] = np.nan  # empty readings
    df["City"] = df["City"].astype(object)
    for i in pick(0.08):                                       # messy city names
        c = df.at[i, "City"]
        df.at[i, "City"] = rng.choice([c.lower(), c.upper(), f" {c} ", "Bangalore" if c == "Bengaluru" else c])
    df = df.astype({"Date": object})
    for i in pick(0.005):                                      # impossible dates
        df.at[i, "Date"] = rng.choice(["2022-13-45", "N/A", "31-02-2023"])
    df = pd.concat([df, df.iloc[pick(0.015)]], ignore_index=True)  # duplicate rows
    return df.sample(frac=1, random_state=seed).reset_index(drop=True)  # shuffle


if __name__ == "__main__":
    out = Path(__file__).resolve().parents[1] / "data" / "raw" / "air_quality_raw.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    data = build()
    data.to_csv(out, index=False)
    print(f"Wrote {len(data)} raw rows to {out}")
