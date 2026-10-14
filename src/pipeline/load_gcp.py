"""Step 3 - LOAD to Google Cloud: Cloud Storage (data lake) and BigQuery (data warehouse).

Needs:  pip install google-cloud-bigquery google-cloud-storage
Login:  gcloud auth application-default login      (see docs/GCP_SETUP.md)
The google libraries are imported inside the functions so the rest of the project runs without them.
"""
from pathlib import Path

from .transform import SCHEMA


def upload_to_gcs(local_path: str, bucket: str, blob_path: str, project_id: str) -> str:
    """Copy a local file into a Cloud Storage bucket and return its gs:// URI."""
    from google.cloud import storage

    client = storage.Client(project=project_id)
    client.bucket(bucket).blob(blob_path).upload_from_filename(local_path)
    return f"gs://{bucket}/{blob_path}"


def _job_config():
    from google.cloud import bigquery

    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # the CSV has a header row
        schema=[bigquery.SchemaField(name, kind) for name, kind in SCHEMA],  # explicit schema, no guessing
        # WRITE_TRUNCATE replaces the table on every run, so re-running never creates duplicates (idempotent)
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # Partition by day: queries that filter on reading_date scan (and cost) less
        time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="reading_date"),
        clustering_fields=["city"],  # sort data inside each partition by city
    )


def load_to_bigquery(csv_path: str, project_id: str, dataset: str, table: str,
                     location: str = "US", gcs_uri: str | None = None) -> int:
    """Create the dataset if needed, load the CSV into a partitioned table and return the row count."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id, location=location)
    ds = bigquery.Dataset(f"{project_id}.{dataset}")
    ds.location = location
    client.create_dataset(ds, exists_ok=True)

    table_id = f"{project_id}.{dataset}.{table}"
    if gcs_uri:  # preferred pattern: warehouse loads from the data lake
        job = client.load_table_from_uri(gcs_uri, table_id, job_config=_job_config())
    else:  # no bucket: upload the file straight from this machine
        with open(Path(csv_path), "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=_job_config())
    job.result()  # wait for the load job to finish (raises if it failed)
    return client.get_table(table_id).num_rows


def run_bigquery_query(sql: str, project_id: str, location: str = "US"):
    """Run a SQL query in BigQuery and return the result as a pandas DataFrame."""
    import pandas as pd
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id, location=location)
    rows = client.query(sql).result()
    return pd.DataFrame([dict(row.items()) for row in rows])
