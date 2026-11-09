"""Settings for the pipeline.

The settings are read from environment variables (or from a .env file),
so project names and bucket names are never written inside the code.
"""
import os

# If the python-dotenv package is installed, read the values from a .env file.
# If it is not installed, we simply skip this step.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def load_settings():
    """Return the settings as a dictionary. Anything that is not set gets a default value."""
    settings = {
        "project_id": os.getenv("GCP_PROJECT_ID", ""),
        "bq_dataset": os.getenv("BQ_DATASET", "air_quality"),
        "bq_table": os.getenv("BQ_TABLE", "daily_air_quality"),
        "bq_location": os.getenv("BQ_LOCATION", "US"),
        "gcs_bucket": os.getenv("GCS_BUCKET", ""),  # empty means: skip Cloud Storage
    }
    return settings
