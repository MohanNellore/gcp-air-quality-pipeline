import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from generate_sample_data import build  # noqa: E402
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

    def test_output_files_written(self):
        self.assertTrue((self.out / "quality_report.json").exists())
        self.assertTrue((self.out / "daily_air_quality_clean.csv").exists())

    def test_cleaning_removed_problems(self):
        r = self.result["report"]
        self.assertGreater(r["rows_raw"], r["rows_clean"])
        self.assertEqual(len(r["cities"]), 6)  # name variants merged into 6 cities

    def test_all_quality_checks_passed(self):
        checks = self.result["report"]["quality_checks"]
        self.assertTrue(checks)
        self.assertTrue(all(c["passed"] for c in checks))


if __name__ == "__main__":
    unittest.main()
