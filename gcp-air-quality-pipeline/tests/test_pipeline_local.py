import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from generate_sample_data import build  # noqa: E402
from pipeline.analytics import list_queries  # noqa: E402
from pipeline.pipeline import run  # noqa: E402


class LocalPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        raw = Path(cls.tmp.name) / "raw.csv"
        build(seed=7, start="2022-01-01", end="2022-12-31").to_csv(raw, index=False)
        cls.result = run(str(raw), mode="local", out_dir=str(Path(cls.tmp.name) / "out"))
        cls.out = Path(cls.tmp.name) / "out"

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_sqlite_supports_window_functions(self):
        self.assertGreaterEqual(tuple(int(x) for x in sqlite3.sqlite_version.split(".")), (3, 25, 0))

    def test_all_queries_ran_and_returned_rows(self):
        self.assertEqual(len(self.result["results"]), len(list_queries()))
        for name, df in self.result["results"].items():
            self.assertGreater(len(df), 0, name)
            self.assertTrue((self.out / "query_results" / f"{name}.csv").exists())

    def test_output_files_written(self):
        self.assertTrue((self.out / "quality_report.json").exists())
        self.assertTrue((self.out / "daily_air_quality_clean.csv").exists())

    def test_cleaning_removed_problems(self):
        r = self.result["report"]
        self.assertGreater(r["rows_raw"], r["rows_clean"])
        self.assertEqual(len(r["cities"]), 6)  # name variants merged into 6 cities

    def test_delhi_is_most_polluted_and_ranked_first(self):
        rank = self.result["results"]["05_city_rank_by_year"]
        self.assertEqual(rank.loc[rank["pollution_rank"] == 1, "city"].iloc[0], "Delhi")
        self.assertEqual(rank["pollution_rank"].min(), 1)

    def test_bucket_shares_sum_to_100_per_city(self):
        share = self.result["results"]["04_aqi_bucket_share"].groupby("city")["pct_of_days"].sum()
        for total in share:
            self.assertAlmostEqual(total, 100.0, delta=0.5)

    def test_worst_days_sorted_descending(self):
        pm = self.result["results"]["02_worst_days"]["pm25"].tolist()
        self.assertEqual(len(pm), 10)
        self.assertEqual(pm, sorted(pm, reverse=True))


if __name__ == "__main__":
    unittest.main()
