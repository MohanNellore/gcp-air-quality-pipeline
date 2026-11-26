"""Submit the Spark job to Dataproc Serverless and wait for it to finish. This part runs on YOUR computer.

Dataproc Serverless runs your PySpark file on Google's machines. There is no cluster to create or
delete: Google starts what the job needs, you pay while it runs, and everything is removed afterwards.
We use the gcloud command (installed with the Google Cloud CLI) to submit the job. It shows
the job's output on your screen while it runs.
"""
import datetime
import os
import shutil
import subprocess
import zipfile

# The files of our package that the Spark job needs. They are put in a zip file and sent with the job.
JOB_FILES = ["__init__.py", "rules.py", "extract.py", "transform.py", "quality.py", "spark_utils.py", "job.py"]


def find_gcloud():
    """Return the path of the gcloud command, or stop with a clear message if it is not installed."""
    path = shutil.which("gcloud")
    if path is None:
        raise RuntimeError("The gcloud command was not found. Install the Google Cloud CLI "
                           "(cloud.google.com/sdk/docs/install), open a new terminal and try again.")
    return path


def make_batch_id():
    """A unique name for this run, like air-quality-20261126-120403 (Dataproc wants lowercase letters, digits, hyphens)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    return "air-quality-" + now.strftime("%Y%m%d-%H%M%S")


def build_code_zip(zip_path):
    """Put the package files the job needs into a zip file. Returns zip_path."""
    package_folder = os.path.dirname(os.path.abspath(__file__))

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for file_name in JOB_FILES:
            # Inside the zip the files live in a folder called "pipeline", so "import pipeline" works
            zip_file.write(os.path.join(package_folder, file_name), "pipeline/" + file_name)

    return zip_path


def make_submit_command(gcloud, settings, batch_id, main_file_uri, zip_uri, job_arguments):
    """Build the gcloud command that submits the job, as a list of words."""
    command = [
        gcloud, "dataproc", "batches", "submit", "pyspark", main_file_uri,
        "--project=" + settings["project_id"],
        "--region=" + settings["dataproc_region"],
        "--batch=" + batch_id,
        "--deps-bucket=gs://" + settings["gcs_bucket"],
        "--py-files=" + zip_uri,
        "--version=" + settings["dataproc_runtime"],
        "--",  # everything after this is given to our job
    ]
    return command + job_arguments


def submit_batch(settings, batch_id, main_file_uri, zip_uri, job_arguments):
    """Submit the job to Dataproc Serverless and wait until it finishes. Raises an error if it fails."""
    command = make_submit_command(find_gcloud(), settings, batch_id, main_file_uri, zip_uri, job_arguments)

    print("        running: gcloud " + " ".join(command[1:]))
    print("        (Dataproc needs a minute or two to start, then the job's output appears below)\n")

    result = subprocess.run(command)  # the output of the job is shown on your screen

    if result.returncode != 0:
        raise RuntimeError(
            "The Dataproc job " + batch_id + " failed. See the output above. You can also open it in the console "
            "(Dataproc -> Serverless -> Batches) or run: gcloud dataproc batches describe " + batch_id
            + " --region=" + settings["dataproc_region"]
            + ". If the error mentions the subnet or Private Google Access, see docs/GCP_SETUP.md (Common problems).")
