"""The pipeline manager. It runs on YOUR computer and does the steps in order:

  1. upload   copy the raw file and the job code into your Cloud Storage bucket
  2. dataproc run the Spark job on Dataproc Serverless (extract -> transform -> quality gate -> write)
  3. load     load the clean data from Cloud Storage into BigQuery
  4. download save the quality report and the clean CSV in the outputs folder

The Spark work itself happens inside the job (see job.py and jobs/air_quality_job.py).
"""
import json
import os
import tempfile

from . import dataproc, load_gcp
from .config import load_settings


def check_settings(settings):
    """Stop early, with a clear message, if a setting is missing."""
    if settings["project_id"] == "":
        raise ValueError("Set GCP_PROJECT_ID (see .env.example)")
    if settings["gcs_bucket"] == "":
        raise ValueError("Set GCS_BUCKET (see .env.example). The raw data, the code and the results live in this bucket")


def run(input_path, settings=None, out_dir="outputs"):
    """Run the whole pipeline and return the data-quality report (a dictionary).

    input_path: the raw CSV file on this computer
    settings:   a dictionary from config.load_settings(). If not given, it is read from the environment.
    out_dir:    the folder on this computer where the report and the clean CSV are saved
    """
    if settings is None:
        settings = load_settings()

    check_settings(settings)
    dataproc.find_gcloud()  # stop now if the Google Cloud CLI is missing, before anything is uploaded

    project = settings["project_id"]
    bucket = settings["gcs_bucket"]
    processed_folder = "processed/daily_air_quality_clean"
    report_folder = "reports/quality_report"

    os.makedirs(out_dir, exist_ok=True)
    clean_csv_path = os.path.join(out_dir, "daily_air_quality_clean.csv")
    report_path = os.path.join(out_dir, "quality_report.json")

    # ----- Step 1: Upload -----
    print("[1/4] Upload    copying the raw file and the job code to gs://" + bucket)
    raw_uri = load_gcp.upload_to_gcs(input_path, bucket, "raw/" + os.path.basename(input_path), project)

    project_folder = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    main_file_uri = load_gcp.upload_to_gcs(
        os.path.join(project_folder, "jobs", "air_quality_job.py"), bucket, "code/air_quality_job.py", project)

    with tempfile.TemporaryDirectory() as temp_folder:
        zip_path = dataproc.build_code_zip(os.path.join(temp_folder, "pipeline.zip"))
        zip_uri = load_gcp.upload_to_gcs(zip_path, bucket, "code/pipeline.zip", project)

    # ----- Step 2: Dataproc -----
    batch_id = dataproc.make_batch_id()
    print("[2/4] Dataproc  running the Spark job on Dataproc Serverless (batch " + batch_id + ")")
    job_arguments = [
        "--input", raw_uri,
        "--processed", "gs://" + bucket + "/" + processed_folder,
        "--report", "gs://" + bucket + "/" + report_folder,
    ]
    try:
        dataproc.submit_batch(settings, batch_id, main_file_uri, zip_uri, job_arguments)
    except RuntimeError:
        # The job writes its report even when a quality check fails. Fetch it so you can read it.
        print("\nThe job failed. Trying to save its quality report in " + out_dir + " ...")
        try:
            load_gcp.download_from_gcs(bucket, report_folder, report_path, project, extension=".txt")
            print("Saved " + report_path)
        except Exception:
            print("(no report was written)")
        raise

    # ----- Step 3: Load into BigQuery (only reached if the job and its quality checks passed) -----
    print("[3/4] Load      loading into BigQuery")
    rows_in_table = load_gcp.load_to_bigquery(
        "gs://" + bucket + "/" + processed_folder + "/part-*.csv",
        project, settings["bq_dataset"], settings["bq_table"], settings["bq_location"])

    # ----- Step 4: Download -----
    print("[4/4] Download  saving the report and the clean CSV in " + out_dir)
    load_gcp.download_from_gcs(bucket, report_folder, report_path, project, extension=".txt")
    load_gcp.download_from_gcs(bucket, processed_folder, clean_csv_path, project)

    with open(report_path, encoding="utf-8") as file:
        report = json.load(file)
    report["bigquery_rows"] = int(rows_in_table)
    with open(report_path, "w", encoding="utf-8") as file:
        file.write(json.dumps(report, indent=2))

    return report
