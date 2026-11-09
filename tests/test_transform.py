"""Tests for the cleaning rules (transform.py) and the quality checks (quality.py).

Run them with:  python -m unittest discover -s tests -v
"""
import os
import sys
import unittest

import numpy as np
import pandas as pd

# Tell Python where to find our code (the src folder)
project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_folder, "src"))

from pipeline.quality import assert_quality, run_checks
from pipeline.transform import aqi_to_bucket, clean, month_to_season, pm25_to_aqi

COLUMNS = ["City", "Date", "PM2.5", "PM10", "NO2", "SO2", "CO", "O3", "AQI", "AQI_Bucket"]


def make_raw_table():
    """A tiny raw table with 7 rows. Each row has a different problem."""
    rows = [
        # kept (the city name gets cleaned)
        [" delhi ", "2022-01-01", "100", "170", "30", "8", "0.9", "25", "250", "Poor"],
        # same city and day as the row above once the name is cleaned, so it is a duplicate
        ["DELHI", "2022-01-01", "100", "170", "30", "8", "0.9", "25", "250", "Poor"],
        # old city name (Bangalore) and a negative PM10
        ["Bangalore", "2022-01-02", "20", "-5", "20", "7", "0.5", "30", "33.3", None],
        # invalid date
        ["Mumbai", "2022-13-45", "40", "70", "22", "8", "0.6", "30", "70", None],
        # every reading is empty
        ["Chennai", "2022-01-03", None, None, None, None, None, None, None, None],
        # PM2.5 and AQI are both missing, so AQI cannot be calculated
        ["Kolkata", "2022-07-01", None, "50", None, None, None, None, None, None],
        # AQI is missing but PM2.5 is there, so AQI can be calculated
        ["Hyderabad", "2022-01-05", "45", "80", "25", "8", "0.7", "30", None, None],
    ]
    return pd.DataFrame(rows, columns=COLUMNS)


class ConversionTests(unittest.TestCase):
    def test_pm25_to_aqi(self):
        # (PM2.5, expected AQI)
        examples = [(0, 0), (30, 50), (60, 100), (90, 200), (120, 300),
                    (250, 400), (380, 500), (900, 500), (45, 75)]
        for pm25, expected_aqi in examples:
            self.assertAlmostEqual(pm25_to_aqi(pm25), expected_aqi, places=1)

    def test_pm25_to_aqi_bad_values(self):
        self.assertTrue(np.isnan(pm25_to_aqi(np.nan)))
        self.assertTrue(np.isnan(pm25_to_aqi(-3)))

    def test_aqi_to_bucket(self):
        self.assertEqual(aqi_to_bucket(50), "Good")
        self.assertEqual(aqi_to_bucket(51), "Satisfactory")
        self.assertEqual(aqi_to_bucket(400), "Very Poor")
        self.assertEqual(aqi_to_bucket(401), "Severe")
        self.assertIsNone(aqi_to_bucket(np.nan))

    def test_month_to_season(self):
        self.assertEqual(month_to_season(1), "Winter")
        self.assertEqual(month_to_season(4), "Summer")
        self.assertEqual(month_to_season(7), "Monsoon")
        self.assertEqual(month_to_season(11), "Post-monsoon")


class CleanTests(unittest.TestCase):
    def setUp(self):
        # This runs before every test: clean the tiny table and keep the results
        self.df, self.report = clean(make_raw_table())

    def test_report_counts(self):
        self.assertEqual(self.report["rows_raw"], 7)
        self.assertEqual(self.report["invalid_dates_dropped"], 1)
        self.assertEqual(self.report["duplicates_removed"], 1)
        self.assertEqual(self.report["negative_values_nulled"], 1)
        self.assertEqual(self.report["empty_rows_dropped"], 1)
        self.assertEqual(self.report["aqi_derived_from_pm25"], 1)  # only Hyderabad
        self.assertEqual(self.report["rows_clean"], 4)

    def test_city_names_are_standardised(self):
        cities = sorted(self.df["city"])
        self.assertEqual(cities, ["Bengaluru", "Delhi", "Hyderabad", "Kolkata"])

    def test_negative_value_becomes_empty(self):
        row = self.df[self.df["city"] == "Bengaluru"].iloc[0]
        self.assertTrue(pd.isna(row["pm10"]))

    def test_aqi_is_calculated_when_missing(self):
        row = self.df[self.df["city"] == "Hyderabad"].iloc[0]
        self.assertEqual(row["aqi"], 75.0)
        self.assertEqual(row["aqi_source"], "derived_from_pm25")
        self.assertEqual(row["aqi_bucket"], "Satisfactory")

    def test_aqi_stays_empty_when_it_cannot_be_calculated(self):
        row = self.df[self.df["city"] == "Kolkata"].iloc[0]
        self.assertTrue(pd.isna(row["aqi"]))
        self.assertTrue(pd.isna(row["aqi_source"]))

    def test_year_month_and_season_are_added(self):
        row = self.df[self.df["city"] == "Kolkata"].iloc[0]
        self.assertEqual(row["reading_year"], 2022)
        self.assertEqual(row["reading_month"], 7)
        self.assertEqual(row["season"], "Monsoon")

    def test_missing_required_column(self):
        table_without_city = pd.DataFrame({"Town": ["x"], "Date": ["2022-01-01"]})
        with self.assertRaises(ValueError):
            clean(table_without_city)

    def test_no_aqi_needs_calculating(self):
        # Every AQI is already in the file, so nothing should be calculated
        table = pd.DataFrame(
            [["Delhi", "2022-01-01", "100", "170", "30", "8", "0.9", "25", "250", "Poor"]],
            columns=COLUMNS)
        df, report = clean(table)
        self.assertEqual(report["aqi_derived_from_pm25"], 0)
        self.assertEqual(df.iloc[0]["aqi_source"], "reported")


class QualityTests(unittest.TestCase):
    def test_clean_data_passes_every_check(self):
        df, report = clean(make_raw_table())
        results = assert_quality(df)
        for name, passed in results:
            self.assertTrue(passed, name)

    def test_duplicates_are_caught(self):
        df, report = clean(make_raw_table())
        # Add a copy of the first row, so one city-day appears twice
        bad_df = pd.concat([df, df.iloc[[0]]], ignore_index=True)

        results = dict(run_checks(bad_df))
        self.assertFalse(results["one row per city per day"])

        with self.assertRaises(ValueError):
            assert_quality(bad_df)


if __name__ == "__main__":
    unittest.main()
