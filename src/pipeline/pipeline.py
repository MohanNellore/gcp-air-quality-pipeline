"""The pipeline manager: runs extract -> transform -> quality gate -> load, in that order."""
import json
import os

from .config import load_settings
from .extract import read_raw_csv
from .local_warehouse import load_dataframe
from .quality import assert_quality
from .transform import clean


def run(input_path, mode="local", out_dir="outputs", settings=None):
    """Run the whole pipeline and return the data-quality report (a dictionary).

    input_path: the raw CSV file
    mode:       "local" (SQLite on this computer) or "gcp" (Cloud Storage + BigQuery)
    out_dir:    the folder where the output files are saved
    settings:   a dictionary from config.load_settings(). If not given, it is read from the environment.
    """
    if settings is None:
        settings = load_settings()

    os.makedirs(out_dir, exist_ok=True)

    # ----- Step 1: Extract -----
    print("[1/4] Extract   reading " + input_path)
    raw = read_raw_csv(input_path)

    # ----- Step 2: Transform -----
    print("[2/4] Transform cleaning and enriching")
    df, report = clean(raw)
    clean_csv_path = os.path.join(out_dir, "daily_air_quality_clean.csv")
    df.to_csv(clean_csv_path, index=False)

    # ----- Step 3: Quality gate (raises an error and stops here if a check fails) -----
    print("[3/4] Quality   running data-quality checks")
    check_results = assert_quality(df)
    report["quality_checks"] = []
    for name, passed in check_results:
        report["quality_checks"].append({"check": name, "passed": passed})

    report_path = os.path.join(out_dir, "quality_report.json")
    with open(report_path, "w", encoding="utf-8") as file:
        file.write(json.dumps(report, indent=2))

    # ----- Step 4: Load -----
    if mode == "local":
        print("[4/4] Load      loading into local SQLite (BigQuery stand-in)")
        connection = load_dataframe(df, settings["bq_table"])
        connection.close()

    elif mode == "gcp":
        # Imported here so that local mode works without the Google libraries installed
        from . import load_gcp

        if settings["project_id"] == "":
            raise ValueError("Set GCP_PROJECT_ID (see .env.example) before running with --mode gcp")

        bucket = settings["gcs_bucket"]
        gcs_uri = None

        if bucket != "":
            print("[4/4] Load      uploading to gs://" + bucket + " and loading into BigQuery")
            # The clean file goes to processed/ and the original file goes to raw/
            gcs_uri = load_gcp.upload_to_gcs(
                clean_csv_path, bucket, "processed/daily_air_quality_clean.csv", settings["project_id"])
            load_gcp.upload_to_gcs(
                input_path, bucket, "raw/" + os.path.basename(input_path), settings["project_id"])
        else:
            print("[4/4] Load      loading into BigQuery (no bucket configured, skipping Cloud Storage)")

        rows_in_table = load_gcp.load_to_bigquery(
            clean_csv_path, settings["project_id"], settings["bq_dataset"],
            settings["bq_table"], settings["bq_location"], gcs_uri)
        report["bigquery_rows"] = int(rows_in_table)

    else:
        raise ValueError("mode must be 'local' or 'gcp'")

    return report
