"""Start the whole pipeline with one command:

  python run_pipeline.py

It uploads the raw file to Cloud Storage, runs the Spark job on Dataproc Serverless, loads the
clean data into BigQuery, and saves the report and the clean CSV in ./outputs/.
Set it up first with docs/GCP_SETUP.md.
"""
import argparse
import os
import sys

# Tell Python where to find our code (the src folder)
project_folder = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_folder, "src"))

# This import must stay below the line above, otherwise Python cannot find "pipeline"
from pipeline.pipeline import run


def main():
    # Read the options typed after the command (--input, --output-dir)
    parser = argparse.ArgumentParser(description="Air-quality batch data pipeline on Dataproc")
    parser.add_argument("--input", default="data/raw/air_quality_raw.csv")
    parser.add_argument("--output-dir", default="outputs")
    args = parser.parse_args()

    report = run(args.input, out_dir=args.output_dir)

    # Print the main numbers from the report
    print("\n=== DATA QUALITY REPORT ===")
    keys_to_show = [
        "rows_raw",
        "missing_city_dropped",
        "invalid_dates_dropped",
        "duplicates_removed",
        "negative_values_nulled",
        "empty_rows_dropped",
        "aqi_derived_from_pm25",
        "rows_clean",
        "bigquery_rows",
    ]
    for key in keys_to_show:
        print(key.ljust(26), report[key])

    print("\nResults saved in ./" + args.output_dir + "/ (daily_air_quality_clean.csv, quality_report.json)")


if __name__ == "__main__":
    main()
