"""Run the pipeline.

  python run_pipeline.py --mode local     # no cloud account needed
  python run_pipeline.py --mode gcp       # Cloud Storage + BigQuery (see docs/GCP_SETUP.md)
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from pipeline.pipeline import run  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description="Air-quality batch data pipeline")
    p.add_argument("--mode", choices=["local", "gcp"], default="local")
    p.add_argument("--input", default="data/raw/air_quality_raw.csv")
    p.add_argument("--output-dir", default="outputs")
    a = p.parse_args()

    result = run(a.input, a.mode, a.output_dir)
    r = result["report"]
    print("\n=== DATA QUALITY REPORT ===")
    for key in ("rows_raw", "missing_city_dropped", "invalid_dates_dropped", "duplicates_removed",
                "negative_values_nulled", "empty_rows_dropped", "aqi_derived_from_pm25", "rows_clean"):
        print(f"{key:26} {r[key]}")
    print(f"\nResults saved in ./{a.output_dir}/ (quality_report.json, query_results/*.csv)")
    for name, df in result["results"].items():
        print(f"\n--- {name} ---")
        print(df.head(8).to_string(index=False))


if __name__ == "__main__":
    main()
