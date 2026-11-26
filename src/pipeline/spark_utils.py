"""Small helpers for Spark: start a session and save results. They run inside the Spark job."""


def get_spark():
    """Start Spark (or reuse the one that is already running) and return the session.

    The session is the entry point to everything in Spark. On Dataproc, Spark is already
    set up for the job, so this simply connects to it. On your own computer it starts a small Spark.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("air-quality-pipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # hide the very long info messages

    # Newer Spark versions raise an error when text cannot be turned into a number or a date.
    # We want an empty value (null) instead, so the cleaning rules can count and handle those rows.
    spark.conf.set("spark.sql.ansi.enabled", "false")
    # Our data is small, so a few pieces are enough (the default is 200, which is slow for small data)
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    return spark


def write_csv_folder(df, folder_path):
    """Save a Spark DataFrame as CSV (with a header row) into a folder, as ONE part file.

    Spark always writes a folder. coalesce(1) puts everything into one piece, so the folder
    holds a single part-00000-....csv file. The folder can be a gs:// path: then Spark
    writes straight into Cloud Storage. mode="overwrite" makes a re-run replace the old
    files, so running the pipeline twice never leaves duplicates.
    coalesce(1) is fine for a small table like this one. For big data you would keep the
    many part files (or write Parquet) so that Spark can write them in parallel.
    """
    df.coalesce(1).write.mode("overwrite").csv(folder_path, header=True)


def write_text_file(spark, text, folder_path):
    """Save a piece of text (for example the JSON report) into a folder, as one part file."""
    one_row = spark.createDataFrame([(text,)], ["value"])
    one_row.coalesce(1).write.mode("overwrite").text(folder_path)
