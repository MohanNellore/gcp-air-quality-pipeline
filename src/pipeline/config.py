"""Settings come from environment variables (or a local .env file), never from the code."""
import os
from dataclasses import dataclass

try:  # optional: load variables from a .env file if python-dotenv is installed
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


@dataclass
class Settings:
    project_id: str = ""
    bq_dataset: str = "air_quality"
    bq_table: str = "daily_air_quality"
    bq_location: str = "US"
    gcs_bucket: str = ""  # optional: leave empty to skip Cloud Storage

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            project_id=os.getenv("GCP_PROJECT_ID", ""),
            bq_dataset=os.getenv("BQ_DATASET", "air_quality"),
            bq_table=os.getenv("BQ_TABLE", "daily_air_quality"),
            bq_location=os.getenv("BQ_LOCATION", "US"),
            gcs_bucket=os.getenv("GCS_BUCKET", ""),
        )

    @property
    def table_id(self) -> str:
        return f"{self.project_id}.{self.bq_dataset}.{self.bq_table}"
