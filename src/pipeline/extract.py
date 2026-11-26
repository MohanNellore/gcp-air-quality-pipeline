"""Step 1 - EXTRACT: read the raw CSV file exactly as it arrived (no cleaning here)."""
import os


def read_raw_csv(spark, path):
    """Read the CSV file with Spark and return it as a Spark DataFrame."""
    # A path like gs://bucket/file.csv is in Cloud Storage (not on this computer).
    # Spark itself reports an error if it is missing.
    if "://" not in path and not os.path.exists(path):
        raise FileNotFoundError("Raw file not found: " + path)

    # header=True: the first row holds the column names.
    # inferSchema=False: read every column as plain text. We do not let Spark guess the
    # types because it can guess wrong. Changing text into numbers and dates is done
    # later, in transform.py.
    df = spark.read.csv(path, header=True, inferSchema=False)

    # take(1) asks Spark for at most one row. If nothing comes back, the file has no data.
    if len(df.take(1)) == 0:
        raise ValueError("Raw file is empty: " + path)

    return df
