"""The work that runs INSIDE Spark (on Dataproc): extract -> transform -> quality gate -> write.

jobs/air_quality_job.py calls run_job() from here. The paths can be gs:// paths (Cloud Storage),
or normal folders if you run the job on your own computer.
"""
import json

from .extract import read_raw_csv
from .quality import run_checks
from .spark_utils import write_csv_folder, write_text_file
from .transform import clean


def run_job(spark, input_path, processed_folder, report_folder):
    """Clean the raw file, check its quality, and write the results. Returns the report (a dictionary).

    input_path:       the raw CSV file
    processed_folder: the folder where the clean CSV is written (only if every quality check passes)
    report_folder:    the folder where the JSON report is written (always)
    """
    # ----- Step 1: Extract -----
    print("[1/3] Extract   reading " + input_path)
    raw = read_raw_csv(spark, input_path)

    # ----- Step 2: Transform -----
    print("[2/3] Transform cleaning and enriching with Spark")
    df, report = clean(raw)

    # ----- Step 3: Quality gate, then write -----
    print("[3/3] Quality   running data-quality checks")
    results = run_checks(df)

    report["quality_checks"] = []
    failed = []
    for name, passed in results:
        report["quality_checks"].append({"check": name, "passed": passed})
        if not passed:
            failed.append(name)

    # The report is always written, even when a check fails, so you can see what went wrong
    write_text_file(spark, json.dumps(report, indent=2), report_folder)

    print("\n=== DATA QUALITY REPORT ===")
    for key in ["rows_raw", "missing_city_dropped", "invalid_dates_dropped", "duplicates_removed",
                "negative_values_nulled", "empty_rows_dropped", "aqi_derived_from_pm25", "rows_clean"]:
        print(key.ljust(26) + str(report[key]))

    # A failed check stops the job here, so bad data is never written to the processed folder
    if len(failed) > 0:
        raise ValueError("Data quality checks failed: " + str(failed))

    write_csv_folder(df, processed_folder)
    print("\nClean data written to " + processed_folder)
    return report
