"""Tests for dataproc.py (building and submitting the Dataproc job). They do not need Spark or Google.

Run them with:  python -m unittest discover -s tests -v
"""
import ast
import os
import re
import subprocess
import sys
import tempfile
import unittest
import zipfile
from unittest import mock

# Tell Python where to find our code (the src folder)
project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_folder, "src"))

from pipeline import dataproc

SETTINGS = {
    "project_id": "demo-project", "gcs_bucket": "demo-bucket",
    "dataproc_region": "us-central1", "dataproc_runtime": "2.3",
}


class BatchIdTests(unittest.TestCase):
    def test_batch_id_follows_the_dataproc_naming_rules(self):
        batch_id = dataproc.make_batch_id()
        # lowercase letters, digits and hyphens only, 4 to 63 characters, starting with a letter
        self.assertTrue(re.fullmatch(r"[a-z][a-z0-9-]{3,62}", batch_id), batch_id)
        self.assertTrue(re.fullmatch(r"air-quality-\d{8}-\d{6}", batch_id), batch_id)


class CodeZipTests(unittest.TestCase):
    def test_zip_holds_the_job_files_inside_a_pipeline_folder(self):
        with tempfile.TemporaryDirectory() as folder:
            zip_path = dataproc.build_code_zip(os.path.join(folder, "pipeline.zip"))
            with zipfile.ZipFile(zip_path) as zip_file:
                names = sorted(zip_file.namelist())
        self.assertEqual(names, sorted("pipeline/" + name for name in dataproc.JOB_FILES))

    def test_the_zip_can_be_imported(self):
        # Import two of the modules straight from the zip, like Python does on Dataproc
        with tempfile.TemporaryDirectory() as folder:
            zip_path = dataproc.build_code_zip(os.path.join(folder, "pipeline.zip"))
            code = ("import sys; sys.path.insert(0, sys.argv[1]); "
                    "import pipeline.rules, pipeline.extract; print(len(pipeline.rules.FINAL_COLUMNS))")
            result = subprocess.run([sys.executable, "-I", "-c", code, zip_path],
                                    capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "14")

    def test_every_file_the_job_imports_is_in_the_zip(self):
        # Read each job file and collect the modules it imports with "from .name import ..."
        package_folder = os.path.join(project_folder, "src", "pipeline")
        for file_name in dataproc.JOB_FILES:
            with open(os.path.join(package_folder, file_name), encoding="utf-8") as file:
                tree = ast.parse(file.read())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.level == 1 and node.module:
                    self.assertIn(node.module + ".py", dataproc.JOB_FILES,
                                  file_name + " imports ." + node.module + " which is not in the zip")


class SubmitCommandTests(unittest.TestCase):
    def test_command_has_the_right_flags_and_job_arguments(self):
        command = dataproc.make_submit_command(
            "gcloud", SETTINGS, "air-quality-20261126-120000",
            "gs://demo-bucket/code/air_quality_job.py", "gs://demo-bucket/code/pipeline.zip",
            ["--input", "gs://demo-bucket/raw/x.csv"])
        self.assertEqual(command, [
            "gcloud", "dataproc", "batches", "submit", "pyspark", "gs://demo-bucket/code/air_quality_job.py",
            "--project=demo-project", "--region=us-central1", "--batch=air-quality-20261126-120000",
            "--deps-bucket=gs://demo-bucket", "--py-files=gs://demo-bucket/code/pipeline.zip",
            "--version=2.3", "--", "--input", "gs://demo-bucket/raw/x.csv"])

    def test_successful_job(self):
        with mock.patch.object(dataproc, "find_gcloud", return_value="gcloud"), \
                mock.patch.object(dataproc.subprocess, "run") as run:
            run.return_value.returncode = 0
            dataproc.submit_batch(SETTINGS, "air-quality-1", "gs://b/main.py", "gs://b/p.zip", ["--a", "b"])
        self.assertEqual(run.call_args.args[0][:5], ["gcloud", "dataproc", "batches", "submit", "pyspark"])

    def test_failed_job_raises_with_a_helpful_message(self):
        with mock.patch.object(dataproc, "find_gcloud", return_value="gcloud"), \
                mock.patch.object(dataproc.subprocess, "run") as run:
            run.return_value.returncode = 1
            with self.assertRaises(RuntimeError) as error:
                dataproc.submit_batch(SETTINGS, "air-quality-1", "gs://b/main.py", "gs://b/p.zip", [])
        message = str(error.exception)
        self.assertIn("air-quality-1", message)
        self.assertIn("gcloud dataproc batches describe air-quality-1 --region=us-central1", message)

    def test_missing_gcloud_gives_a_clear_message(self):
        with mock.patch.object(dataproc.shutil, "which", return_value=None):
            with self.assertRaises(RuntimeError) as error:
                dataproc.find_gcloud()
        self.assertIn("Google Cloud CLI", str(error.exception))


if __name__ == "__main__":
    unittest.main()
