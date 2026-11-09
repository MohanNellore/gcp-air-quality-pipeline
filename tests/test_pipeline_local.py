"""Test the whole pipeline in local mode (no cloud account needed).

Run it with:  python -m unittest discover -s tests -v
"""
import os
import sys
import tempfile
import unittest

# Tell Python where to find our code (the src and scripts folders)
project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_folder, "src"))
sys.path.insert(0, os.path.join(project_folder, "scripts"))

from generate_sample_data import build
from pipeline.pipeline import run


class LocalPipelineTests(unittest.TestCase):
    def setUp(self):
        # This runs before every test: make messy data in a temporary folder and run the pipeline on it
        self.temp_folder = tempfile.TemporaryDirectory()
        raw_path = os.path.join(self.temp_folder.name, "raw.csv")
        self.out_folder = os.path.join(self.temp_folder.name, "out")

        raw_table = build(seed=7, start="2022-01-01", end="2022-12-31")
        raw_table.to_csv(raw_path, index=False)

        self.report = run(raw_path, mode="local", out_dir=self.out_folder)

    def tearDown(self):
        # This runs after every test: delete the temporary folder
        self.temp_folder.cleanup()

    def test_output_files_are_written(self):
        self.assertTrue(os.path.exists(os.path.join(self.out_folder, "quality_report.json")))
        self.assertTrue(os.path.exists(os.path.join(self.out_folder, "daily_air_quality_clean.csv")))

    def test_cleaning_removed_the_problems(self):
        self.assertGreater(self.report["rows_raw"], self.report["rows_clean"])
        # The messy name variants (bangalore, DELHI, ...) are merged into 6 cities
        self.assertEqual(len(self.report["cities"]), 6)

    def test_all_quality_checks_passed(self):
        checks = self.report["quality_checks"]
        self.assertGreater(len(checks), 0)
        for check in checks:
            self.assertTrue(check["passed"], check["check"])


if __name__ == "__main__":
    unittest.main()
