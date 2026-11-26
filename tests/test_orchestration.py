"""Tests for the pipeline manager (pipeline.py) with the cloud parts replaced by stand-ins.
They check the order of the steps, the paths and what happens when the Dataproc job fails.
They do not need Spark, Java or a Google account.

Run them with:  python -m unittest discover -s tests -v
"""
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
import zipfile
from contextlib import redirect_stdout
from unittest import mock

# Tell Python where to find our code (the src folder)
project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_folder, "src"))

from pipeline import pipeline as manager

SETTINGS = {
    "project_id": "demo-project", "bq_dataset": "air_quality", "bq_table": "daily_air_quality",
    "bq_location": "US", "gcs_bucket": "demo-bucket", "dataproc_region": "us-central1",
    "dataproc_runtime": "2.3",
}
REPORT_FROM_JOB = {"rows_raw": 5, "missing_city_dropped": 0, "invalid_dates_dropped": 1,
                   "duplicates_removed": 1, "negative_values_nulled": 0, "empty_rows_dropped": 1,
                   "aqi_derived_from_pm25": 0, "rows_clean": 1}


class FakeCloud:
    """Stands in for Cloud Storage, Dataproc and BigQuery, and records what the manager does."""

    def __init__(self, job_fails=False, report_available=True):
        self.events = []
        self.job_fails = job_fails
        self.report_available = report_available
        self.job_arguments = None
        self.zip_was_valid = None

    def upload_to_gcs(self, local_path, bucket, blob_path, project):
        self.events.append("upload " + blob_path)
        if blob_path == "code/pipeline.zip":
            self.zip_was_valid = zipfile.is_zipfile(local_path)  # the file must exist at this moment
        return "gs://" + bucket + "/" + blob_path

    def submit_batch(self, settings, batch_id, main_file_uri, zip_uri, job_arguments):
        self.events.append("dataproc")
        self.job_arguments = job_arguments
        self.uris = (main_file_uri, zip_uri)
        if self.job_fails:
            raise RuntimeError("The Dataproc job failed")

    def load_to_bigquery(self, gcs_uri, project, dataset, table, location):
        self.events.append("bigquery " + gcs_uri)
        return 4272

    def download_from_gcs(self, bucket, folder, local_path, project, extension=".csv"):
        self.events.append("download " + folder)
        if extension == ".txt":
            if not self.report_available:
                raise RuntimeError("no part file")
            with open(local_path, "w", encoding="utf-8") as file:
                file.write(json.dumps(REPORT_FROM_JOB))
        else:
            with open(local_path, "w", encoding="utf-8") as file:
                file.write("city,reading_date\n")

    def patches(self):
        return [
            mock.patch.object(manager.load_gcp, "upload_to_gcs", self.upload_to_gcs),
            mock.patch.object(manager.load_gcp, "load_to_bigquery", self.load_to_bigquery),
            mock.patch.object(manager.load_gcp, "download_from_gcs", self.download_from_gcs),
            mock.patch.object(manager.dataproc, "submit_batch", self.submit_batch),
            mock.patch.object(manager.dataproc, "find_gcloud", return_value="gcloud"),
        ]

    def run_pipeline(self, out_dir, settings=SETTINGS):
        patches = self.patches()
        for patch in patches:
            patch.start()
        try:
            with redirect_stdout(io.StringIO()):
                return manager.run("data/raw/air_quality_raw.csv", settings, out_dir)
        finally:
            for patch in patches:
                patch.stop()


class OrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_folder = tempfile.TemporaryDirectory()
        self.out_dir = os.path.join(self.temp_folder.name, "outputs")

    def tearDown(self):
        self.temp_folder.cleanup()

    def test_steps_run_in_order(self):
        cloud = FakeCloud()
        cloud.run_pipeline(self.out_dir)
        self.assertEqual(cloud.events, [
            "upload raw/air_quality_raw.csv", "upload code/air_quality_job.py", "upload code/pipeline.zip",
            "dataproc",
            "bigquery gs://demo-bucket/processed/daily_air_quality_clean/part-*.csv",
            "download reports/quality_report", "download processed/daily_air_quality_clean"])

    def test_the_job_gets_the_right_files_and_arguments(self):
        cloud = FakeCloud()
        cloud.run_pipeline(self.out_dir)
        self.assertEqual(cloud.uris, ("gs://demo-bucket/code/air_quality_job.py", "gs://demo-bucket/code/pipeline.zip"))
        self.assertEqual(cloud.job_arguments, [
            "--input", "gs://demo-bucket/raw/air_quality_raw.csv",
            "--processed", "gs://demo-bucket/processed/daily_air_quality_clean",
            "--report", "gs://demo-bucket/reports/quality_report"])
        self.assertTrue(cloud.zip_was_valid)

    def test_report_and_csv_are_saved_and_the_report_gets_the_bigquery_row_count(self):
        report = FakeCloud().run_pipeline(self.out_dir)
        self.assertEqual(report["bigquery_rows"], 4272)
        self.assertEqual(report["rows_clean"], 1)
        with open(os.path.join(self.out_dir, "quality_report.json"), encoding="utf-8") as file:
            self.assertEqual(json.load(file)["bigquery_rows"], 4272)
        self.assertTrue(os.path.exists(os.path.join(self.out_dir, "daily_air_quality_clean.csv")))

    def test_failed_job_never_loads_bigquery_and_still_fetches_the_report(self):
        cloud = FakeCloud(job_fails=True)
        with self.assertRaises(RuntimeError):
            cloud.run_pipeline(self.out_dir)
        self.assertNotIn("bigquery", " ".join(cloud.events))
        self.assertIn("download reports/quality_report", cloud.events)
        self.assertTrue(os.path.exists(os.path.join(self.out_dir, "quality_report.json")))

    def test_failed_job_without_a_report_still_raises_the_job_error(self):
        cloud = FakeCloud(job_fails=True, report_available=False)
        with self.assertRaises(RuntimeError) as error:
            cloud.run_pipeline(self.out_dir)
        self.assertIn("Dataproc job failed", str(error.exception))

    def test_missing_settings_stop_before_anything_is_uploaded(self):
        for changes in ({"project_id": ""}, {"gcs_bucket": ""}):
            cloud = FakeCloud()
            with self.assertRaises(ValueError):
                cloud.run_pipeline(self.out_dir, dict(SETTINGS, **changes))
            self.assertEqual(cloud.events, [])

    def test_missing_gcloud_stops_before_anything_is_uploaded(self):
        cloud = FakeCloud()
        patches = cloud.patches()[:-1] + [
            mock.patch.object(manager.dataproc, "find_gcloud", side_effect=RuntimeError("gcloud not found"))]
        for patch in patches:
            patch.start()
        try:
            with self.assertRaises(RuntimeError):
                manager.run("data/raw/air_quality_raw.csv", SETTINGS, self.out_dir)
        finally:
            for patch in patches:
                patch.stop()
        self.assertEqual(cloud.events, [])


class EntryPointTests(unittest.TestCase):
    def test_run_pipeline_prints_the_report(self):
        spec = importlib.util.spec_from_file_location("run_pipeline", os.path.join(project_folder, "run_pipeline.py"))
        entry_point = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(entry_point)

        report = dict(REPORT_FROM_JOB, bigquery_rows=4272)
        output = io.StringIO()
        with mock.patch.object(entry_point, "run", return_value=report), \
                mock.patch.object(sys, "argv", ["run_pipeline.py"]), redirect_stdout(output):
            entry_point.main()
        self.assertIn("DATA QUALITY REPORT", output.getvalue())
        self.assertIn("bigquery_rows", output.getvalue())


if __name__ == "__main__":
    unittest.main()
