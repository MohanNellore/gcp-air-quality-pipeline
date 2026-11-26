"""Google Cloud helpers: Cloud Storage (data lake) and BigQuery (data warehouse).

Needs:  pip install google-cloud-bigquery google-cloud-storage
Login:  gcloud auth application-default login      (see docs/GCP_SETUP.md)

The Google libraries are imported inside the functions (not at the top of the file),
so the rest of the project still runs on a computer that does not have them installed.
"""
from .rules import SCHEMA


def upload_to_gcs(local_path, bucket_name, blob_path, project_id):
    """Copy a file from this computer into a Cloud Storage bucket. Returns its gs:// address."""
    from google.cloud import storage

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)

    return "gs://" + bucket_name + "/" + blob_path


def download_from_gcs(bucket_name, folder_path, local_path, project_id, extension=".csv"):
    """Copy the one part file that Spark wrote into a Cloud Storage folder to this computer."""
    from google.cloud import storage

    client = storage.Client(project=project_id)

    part_blobs = []
    for blob in client.list_blobs(bucket_name, prefix=folder_path.rstrip("/") + "/"):
        file_name = blob.name.split("/")[-1]
        if file_name.startswith("part-") and file_name.endswith(extension):
            part_blobs.append(blob)

    if len(part_blobs) != 1:
        raise RuntimeError("Expected one part file in gs://" + bucket_name + "/" + folder_path
                           + ", found " + str(len(part_blobs)))

    part_blobs[0].download_to_filename(local_path)


def make_load_config():
    """Build the settings for the BigQuery load job."""
    from google.cloud import bigquery

    # Explicit schema: we tell BigQuery every column name and type ourselves,
    # so it does not have to guess.
    schema_fields = []
    for column_name, column_type in SCHEMA:
        schema_fields.append(bigquery.SchemaField(column_name, column_type))

    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # the first row of the CSV is the header
        schema=schema_fields,
        # WRITE_TRUNCATE replaces the table on every run,
        # so running the pipeline twice never creates duplicate rows.
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # Partition by day: a query that filters on reading_date reads less data (and costs less).
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="reading_date",
        ),
        # Cluster by city: inside each partition, rows are stored sorted by city.
        clustering_fields=["city"],
    )
    return config


def load_to_bigquery(gcs_uri, project_id, dataset, table, location="US"):
    """Load the clean CSV from Cloud Storage into a BigQuery table. Returns the number of rows in the table.

    gcs_uri can use one * wildcard, for example gs://bucket/processed/clean/part-*.csv
    (Spark names its output files part-00000-....csv).
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id, location=location)

    # Create the dataset if it does not exist yet
    dataset_object = bigquery.Dataset(project_id + "." + dataset)
    dataset_object.location = location
    client.create_dataset(dataset_object, exists_ok=True)

    table_id = project_id + "." + dataset + "." + table
    config = make_load_config()

    # BigQuery reads the file straight from Cloud Storage
    job = client.load_table_from_uri(gcs_uri, table_id, job_config=config)

    job.result()  # wait until the load finishes (this raises an error if it failed)

    return client.get_table(table_id).num_rows
