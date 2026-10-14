import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline.quality import assert_quality, run_checks  # noqa: E402
from pipeline.transform import aqi_to_bucket, clean, month_to_season, pm25_to_aqi  # noqa: E402

COLS = ["City", "Date", "PM2.5", "PM10", "NO2", "SO2", "CO", "O3", "AQI", "AQI_Bucket"]


def raw_frame():
    rows = [
        [" delhi ", "2022-01-01", "100", "170", "30", "8", "0.9", "25", "250", "Poor"],      # kept (city cleaned)
        ["DELHI", "2022-01-01", "100", "170", "30", "8", "0.9", "25", "250", "Poor"],        # duplicate after cleaning
        ["Bangalore", "2022-01-02", "20", "-5", "20", "7", "0.5", "30", "33.3", None],        # alias + negative PM10
        ["Mumbai", "2022-13-45", "40", "70", "22", "8", "0.6", "30", "70", None],             # invalid date
        ["Chennai", "2022-01-03", None, None, None, None, None, None, None, None],            # empty reading
        ["Kolkata", "2022-07-01", None, "50", None, None, None, None, None, None],            # pm25 and aqi missing
        ["Hyderabad", "2022-01-05", "45", "80", "25", "8", "0.7", "30", None, None],          # AQI derivable from PM2.5
    ]
    return pd.DataFrame(rows, columns=COLS)


class ConversionTests(unittest.TestCase):
    def test_pm25_breakpoints(self):
        for pm, aqi in [(0, 0), (30, 50), (60, 100), (90, 200), (120, 300), (250, 400), (380, 500), (900, 500), (45, 75)]:
            self.assertAlmostEqual(pm25_to_aqi(pm), aqi, places=1, msg=f"pm25={pm}")
        self.assertTrue(np.isnan(pm25_to_aqi(np.nan)))
        self.assertTrue(np.isnan(pm25_to_aqi(-3)))

    def test_buckets(self):
        self.assertEqual(aqi_to_bucket(50), "Good")
        self.assertEqual(aqi_to_bucket(51), "Satisfactory")
        self.assertEqual(aqi_to_bucket(400), "Very Poor")
        self.assertEqual(aqi_to_bucket(401), "Severe")
        self.assertIsNone(aqi_to_bucket(np.nan))

    def test_seasons(self):
        self.assertEqual([month_to_season(m) for m in (1, 4, 7, 11)], ["Winter", "Summer", "Monsoon", "Post-monsoon"])


class CleanTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.df, cls.report = clean(raw_frame())

    def test_report_counts(self):
        r = self.report
        self.assertEqual(r["rows_raw"], 7)
        self.assertEqual(r["invalid_dates_dropped"], 1)
        self.assertEqual(r["duplicates_removed"], 1)
        self.assertEqual(r["negative_values_nulled"], 1)
        self.assertEqual(r["empty_rows_dropped"], 1)
        self.assertEqual(r["aqi_derived_from_pm25"], 1)  # only Hyderabad has PM2.5 but no AQI
        self.assertEqual(r["rows_clean"], 4)

    def test_cities_standardised(self):
        self.assertEqual(sorted(self.df["city"]), ["Bengaluru", "Delhi", "Hyderabad", "Kolkata"])

    def test_negative_value_becomes_null(self):
        row = self.df[self.df["city"] == "Bengaluru"].iloc[0]
        self.assertTrue(pd.isna(row["pm10"]))

    def test_aqi_derived_and_bucketed(self):
        row = self.df[self.df["city"] == "Hyderabad"].iloc[0]
        self.assertEqual(row["aqi"], 75.0)
        self.assertEqual(row["aqi_source"], "derived_from_pm25")
        self.assertEqual(row["aqi_bucket"], "Satisfactory")
        kol = self.df[self.df["city"] == "Kolkata"].iloc[0]
        self.assertTrue(pd.isna(kol["aqi"]))
        self.assertTrue(pd.isna(kol["aqi_source"]))  # null: no PM2.5 and no AQI, so nothing to derive from

    def test_features(self):
        row = self.df[self.df["city"] == "Kolkata"].iloc[0]
        self.assertEqual((row["reading_year"], row["reading_month"], row["season"]), (2022, 7, "Monsoon"))

    def test_missing_required_column(self):
        with self.assertRaises(ValueError):
            clean(pd.DataFrame({"Town": ["x"], "Date": ["2022-01-01"]}))


class QualityTests(unittest.TestCase):
    def test_clean_data_passes(self):
        df, _ = clean(raw_frame())
        self.assertTrue(all(ok for _, ok in assert_quality(df)))

    def test_duplicates_are_caught(self):
        df, _ = clean(raw_frame())
        bad = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        self.assertFalse(dict(run_checks(bad))["one row per city per day"])
        with self.assertRaises(ValueError):
            assert_quality(bad)


if __name__ == "__main__":
    unittest.main()
