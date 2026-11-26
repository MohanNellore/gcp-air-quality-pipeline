"""Tests for the plain-Python rules (rules.py). These do not need Spark.

Run them with:  python -m unittest discover -s tests -v
"""
import os
import sys
import unittest

# Tell Python where to find our code (the src folder)
project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_folder, "src"))

from pipeline.rules import FINAL_COLUMNS, SCHEMA, aqi_to_bucket, month_to_season, pm25_to_aqi


class RulesTests(unittest.TestCase):
    def test_pm25_to_aqi(self):
        # (PM2.5, expected AQI)
        examples = [(0, 0), (30, 50), (60, 100), (90, 200), (120, 300),
                    (250, 400), (380, 500), (900, 500), (45, 75)]
        for pm25, expected_aqi in examples:
            self.assertAlmostEqual(pm25_to_aqi(pm25), expected_aqi, places=1)

    def test_pm25_to_aqi_bad_values(self):
        self.assertIsNone(pm25_to_aqi(None))
        self.assertIsNone(pm25_to_aqi(float("nan")))
        self.assertIsNone(pm25_to_aqi(-3))

    def test_aqi_to_bucket(self):
        self.assertEqual(aqi_to_bucket(50), "Good")
        self.assertEqual(aqi_to_bucket(51), "Satisfactory")
        self.assertEqual(aqi_to_bucket(400), "Very Poor")
        self.assertEqual(aqi_to_bucket(401), "Severe")
        self.assertIsNone(aqi_to_bucket(None))
        self.assertIsNone(aqi_to_bucket(float("nan")))

    def test_month_to_season(self):
        self.assertEqual(month_to_season(1), "Winter")
        self.assertEqual(month_to_season(4), "Summer")
        self.assertEqual(month_to_season(7), "Monsoon")
        self.assertEqual(month_to_season(11), "Post-monsoon")

    def test_final_columns_follow_the_schema(self):
        self.assertEqual(len(FINAL_COLUMNS), 14)
        self.assertEqual(FINAL_COLUMNS, [name for name, column_type in SCHEMA])


if __name__ == "__main__":
    unittest.main()
