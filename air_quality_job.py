"""The air-quality pipeline as ONE PySpark file, to run on Dataproc Serverless in Google Cloud.

What it does, in order:
  1. EXTRACT    read the raw CSV from Cloud Storage
  2. TRANSFORM  clean the data and add new columns
  3. QUALITY    check the clean data (the job stops here if a check fails)
  4. LOAD       save the clean CSV and the quality report in Cloud Storage,
                then load the clean data into BigQuery (partitioned by day, clustered by city)

Arguments (you type them in the Dataproc console, one argument per box):
  --bucket   the name of your Cloud Storage bucket, without gs://
  --table    the BigQuery table, written as PROJECT.DATASET.TABLE
  --input    the raw file inside the bucket (default: raw/air_quality_raw.csv)

Spark is lazy: lines like filter() and withColumn() only write down a plan. Spark does the
real work when we ask for a result with an ACTION such as count(), collect() or write.
"""
import argparse
import json

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# ===========================================================================
# 0. TABLES AND SETTINGS
# ===========================================================================

# The six pollution columns
POLLUTANTS = ["pm25", "pm10", "no2", "so2", "co", "o3"]

# The final table: (column name, column type). Spark and BigQuery use these types.
FINAL_TABLE = [
    ("city", "string"),
    ("reading_date", "date"),
    ("pm25", "double"),
    ("pm10", "double"),
    ("no2", "double"),
    ("so2", "double"),
    ("co", "double"),
    ("o3", "double"),
    ("aqi", "double"),
    ("aqi_bucket", "string"),
    ("aqi_source", "string"),
    ("reading_year", "int"),
    ("reading_month", "int"),
    ("season", "string"),
]

# Just the column names, in the same order
FINAL_COLUMNS = []
for column_name, column_type in FINAL_TABLE:
    FINAL_COLUMNS.append(column_name)

# Old city names and the new name we want instead (keys must be lowercase)
CITY_ALIASES = {
    "bangalore": "Bengaluru",
    "bombay": "Mumbai",
    "madras": "Chennai",
    "calcutta": "Kolkata",
}

# PM2.5 -> AQI conversion table. Each row means:
# (PM2.5 from, PM2.5 to, AQI from, AQI to)
# It is a simplified version of India's CPCB scale, made for learning.
PM25_BREAKPOINTS = [
    (0, 30, 0, 50),
    (30, 60, 50, 100),
    (60, 90, 100, 200),
    (90, 120, 200, 300),
    (120, 250, 300, 400),
    (250, 380, 400, 500),
]

# AQI label table. Each row means: (highest AQI for this label, label)
AQI_BUCKETS = [
    (50, "Good"),
    (100, "Satisfactory"),
    (200, "Moderate"),
    (300, "Poor"),
    (400, "Very Poor"),
]

# Season table. Each row means: (season name, the months in that season)
SEASON_MONTHS = [
    ("Winter", [12, 1, 2]),
    ("Summer", [3, 4, 5]),
    ("Monsoon", [6, 7, 8, 9]),
]
OTHER_SEASON = "Post-monsoon"  # every month not listed above (October, November)


def get_spark():
    """Start Spark (or connect to the one Dataproc already started) and return the session."""
    spark = SparkSession.builder.appName("air-quality-pipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # hide the very long info messages

    # Newer Spark versions raise an error when text cannot be turned into a number or a date.
    # We want an empty value (null) instead, so the cleaning rules can count and handle those rows.
    spark.conf.set("spark.sql.ansi.enabled", "false")
    # Our data is small, so a few pieces are enough (the default is 200, which is slow for small data)
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    return spark


# ===========================================================================
# 1. EXTRACT: read the raw CSV file exactly as it arrived (no cleaning here)
# ===========================================================================

def read_raw_csv(spark, path):
    """Read the CSV file from Cloud Storage and return it as a Spark DataFrame."""
    # header=True: the first row holds the column names.
    # inferSchema=False: read every column as plain text. We do not let Spark guess the
    # types because it can guess wrong. Changing text into numbers and dates is done
    # later, in the TRANSFORM part below.
    df = spark.read.csv(path, header=True, inferSchema=False)

    # take(1) asks Spark for at most one row. If nothing comes back, the file has no data.
    if len(df.take(1)) == 0:
        raise ValueError("Raw file is empty: " + path)

    return df


# ===========================================================================
# 2. TRANSFORM: clean the data and add useful new columns
#
# clean() at the end of this part fixes the data one rule at a time and counts how many
# rows each rule changed. Those counts become the data-quality report.
# ===========================================================================

# ---------------------------------------------------------------------------
# Column helpers. Each one takes a Spark column and returns a new Spark column.
# ---------------------------------------------------------------------------

def empty_text():
    """An empty (null) value for a text column."""
    return F.lit(None).cast("string")


def empty_number():
    """An empty (null) value for a number column."""
    return F.lit(None).cast("double")


def to_number(column):
    """Change text into a number. Text that is not a number becomes empty (null)."""
    number = column.cast("double")
    # Spark turns the text "NaN" into a special not-a-number value. We want a normal empty value.
    return F.when(F.isnan(number), empty_number()).otherwise(number)


def tidy_city_column(city):
    """Return a clean city name, or null if there is no usable name."""
    trimmed = F.trim(city)

    # Other names just get a capital first letter (delhi -> Delhi)
    result = F.initcap(trimmed)

    # Old names are replaced by the new name (Bangalore -> Bengaluru)
    for old_name, new_name in CITY_ALIASES.items():
        result = F.when(F.lower(trimmed) == old_name, new_name).otherwise(result)

    # No name at all means no city
    return F.when(trimmed.isNull() | (trimmed == ""), empty_text()).otherwise(result)


def pm25_to_aqi_column(pm25):
    """Turn a PM2.5 column into an AQI column (same rule as pm25_to_aqi in rules.py)."""
    # PM2.5 above the last row of the table is the worst possible score
    result = F.lit(500.0)

    # We go through the table from the LAST row to the FIRST row and wrap each row around
    # the answer so far. The result is: "if PM2.5 <= 30 use row 1, otherwise if <= 60 use row 2, ..."
    for pm_low, pm_high, aqi_low, aqi_high in reversed(PM25_BREAKPOINTS):
        steps_per_unit = (aqi_high - aqi_low) / (pm_high - pm_low)
        aqi_in_this_row = F.round(F.lit(steps_per_unit) * (pm25 - pm_low) + aqi_low, 1)
        result = F.when(pm25 <= pm_high, aqi_in_this_row).otherwise(result)

    # Empty or negative PM2.5 has no AQI
    return F.when(pm25.isNull() | (pm25 < 0), empty_number()).otherwise(result)


def aqi_to_bucket_column(aqi):
    """Turn an AQI column into a label column (same rule as aqi_to_bucket in rules.py)."""
    result = F.lit("Severe")  # above the last row of the table

    for highest_aqi, label in reversed(AQI_BUCKETS):
        result = F.when(aqi <= highest_aqi, label).otherwise(result)

    return F.when(aqi.isNull(), empty_text()).otherwise(result)


def month_to_season_column(month):
    """Turn a month-number column into a season-name column (same rule as month_to_season in rules.py)."""
    result = F.lit(OTHER_SEASON)

    for season, months in reversed(SEASON_MONTHS):
        result = F.when(month.isin(months), season).otherwise(result)

    return result


# ---------------------------------------------------------------------------
# Cleaning steps. Each one takes the table, fixes one thing, and returns it.
# ---------------------------------------------------------------------------

def standardize_columns(df):
    """Make the column names tidy.

    'PM2.5' becomes 'pm25', 'AQI_Bucket' becomes 'aqi_bucket', 'Date' becomes 'reading_date'.
    """
    new_names = []
    for name in df.columns:
        name = name.strip().lower()
        name = name.replace(".", "")
        name = name.replace(" ", "_")
        new_names.append(name)

    df = df.toDF(*new_names)  # give the table the new names, in the same order
    df = df.withColumnRenamed("date", "reading_date")
    return df


def fix_city_names(df, report):
    """Rule 1: tidy the city names and drop the rows that have no city."""
    df = df.withColumn("city", tidy_city_column(F.col("city")))

    no_city = F.col("city").isNull()
    report["missing_city_dropped"] = df.filter(no_city).count()

    df = df.filter(~no_city)
    return df


def fix_dates(df, report):
    """Rule 2: a date must look like 2024-01-15. Rows with any other date are dropped."""
    # Step 1: the text must have the exact shape 4 digits - 2 digits - 2 digits.
    # Step 2: cast("date") turns it into a real date. An impossible date such as 2022-13-45
    # or 2022-02-30 becomes empty (null), because ANSI mode is switched off in get_spark().
    date_text = F.col("reading_date")
    looks_like_a_date = date_text.rlike(r"^\d{4}-\d{2}-\d{2}$")
    df = df.withColumn("reading_date", F.when(looks_like_a_date, date_text.cast("date")))

    bad_date = F.col("reading_date").isNull()
    report["invalid_dates_dropped"] = df.filter(bad_date).count()

    df = df.filter(~bad_date)
    return df


def fix_numbers(df, report):
    """Rule 3: change the text into numbers.

    A pollution value below zero is impossible, so we replace it with an
    empty value (null). The row itself is kept.
    """
    number_columns = POLLUTANTS + ["aqi"]

    for column in number_columns:
        df = df.withColumn(column, to_number(F.col(column)))

    # Count the negative values in all columns with one pass over the data
    counts = []
    for column in number_columns:
        counts.append(F.count(F.when(F.col(column) < 0, True)).alias(column))
    negatives_per_column = df.select(counts).first()
    report["negative_values_nulled"] = sum(negatives_per_column)

    # Keep a value only if it is zero or more. Everything else becomes empty.
    for column in number_columns:
        df = df.withColumn(column, F.when(F.col(column) >= 0, F.col(column)))

    return df


def remove_duplicates(df, report):
    """Rule 4: keep only one reading for each city on each day."""
    # Number the rows inside each (city, day) group, in the order they were in the file.
    # The first row of each group is number 1. Any row with a higher number is a duplicate.
    same_city_and_day = Window.partitionBy("city", "reading_date").orderBy("row_id")
    df = df.withColumn("copy_number", F.row_number().over(same_city_and_day))

    is_duplicate = F.col("copy_number") > 1
    report["duplicates_removed"] = df.filter(is_duplicate).count()

    df = df.filter(~is_duplicate).drop("copy_number")
    return df


def remove_empty_rows(df, report):
    """Rule 5: drop rows that have none of the six pollution values. They carry no information."""
    has_no_data = F.lit(True)
    for column in POLLUTANTS:
        has_no_data = has_no_data & F.col(column).isNull()

    report["empty_rows_dropped"] = df.filter(has_no_data).count()

    df = df.filter(~has_no_data)
    return df


def fill_aqi(df, report):
    """Rule 6: fill a missing AQI from PM2.5, and rebuild the AQI label.

    The new column aqi_source tells us where each AQI came from:
    'reported' (it was in the file) or 'derived_from_pm25' (we calculated it).
    """
    # Rows where AQI is empty but PM2.5 is available
    df = df.withColumn("can_derive", F.col("aqi").isNull() & F.col("pm25").isNotNull())
    report["aqi_derived_from_pm25"] = df.filter(F.col("can_derive")).count()

    df = df.withColumn(
        "aqi",
        F.when(F.col("can_derive"), pm25_to_aqi_column(F.col("pm25"))).otherwise(F.col("aqi")))

    df = df.withColumn(
        "aqi_source",
        F.when(F.col("aqi").isNull(), empty_text())  # no AQI at all, so no source
        .when(F.col("can_derive"), "derived_from_pm25")
        .otherwise("reported"))

    df = df.withColumn("aqi_bucket", aqi_to_bucket_column(F.col("aqi")))
    df = df.drop("can_derive")
    return df


def add_features(df):
    """Rule 7: add year, month and season columns, and tidy the formats."""
    df = df.withColumn("reading_year", F.year("reading_date"))
    df = df.withColumn("reading_month", F.month("reading_date"))
    df = df.withColumn("season", month_to_season_column(F.col("reading_month")))

    # Keep two digits after the decimal point
    for column in POLLUTANTS + ["aqi"]:
        df = df.withColumn(column, F.round(F.col(column), 2))

    return df


# ---------------------------------------------------------------------------
# The main function
# ---------------------------------------------------------------------------

def clean(raw):
    """Clean the raw table.

    Returns two things: the clean table (a Spark DataFrame), and a report (a dictionary of counts).
    """
    df = standardize_columns(raw)

    # City and date are required. Without them we cannot continue.
    for column in ["city", "reading_date"]:
        if column not in df.columns:
            raise ValueError("Required column missing in raw data: " + column)

    # Give every row a number: its position in the file. Rule 4 uses it to know which
    # duplicate came first. cache() keeps the numbered table in memory, so the numbers
    # stay the same every time Spark needs the table again.
    df = df.withColumn("row_id", F.monotonically_increasing_id()).cache()

    report = {"rows_raw": df.count()}

    # If a pollution column is missing from the file, create it empty
    # instead of crashing.
    for column in POLLUTANTS + ["aqi"]:
        if column not in df.columns:
            df = df.withColumn(column, empty_text())

    df = fix_city_names(df, report)
    df = fix_dates(df, report)
    df = fix_numbers(df, report)
    df = remove_duplicates(df, report)
    df = remove_empty_rows(df, report)
    df = fill_aqi(df, report)
    df = add_features(df)

    # Keep only the final columns, in the right order, with the right types, sorted by city and date.
    # cache() so the checks and the file writing below do not repeat all the work above.
    columns = []
    for column_name, column_type in FINAL_TABLE:
        columns.append(F.col(column_name).cast(column_type).alias(column_name))
    df = df.select(columns).orderBy("city", "reading_date").cache()

    # Finish the report
    report["rows_clean"] = df.count()

    city_rows = df.select("city").distinct().orderBy("city").collect()
    report["cities"] = [row["city"] for row in city_rows]

    first_date, last_date = df.agg(
        F.date_format(F.min("reading_date"), "yyyy-MM-dd"),
        F.date_format(F.max("reading_date"), "yyyy-MM-dd")).first()
    report["date_range"] = [first_date, last_date]

    # Count the empty values left in each column (only columns that have some)
    empty_counts = []
    for column in df.columns:
        empty_counts.append(F.count(F.when(F.col(column).isNull(), True)).alias(column))
    counts_row = df.select(empty_counts).first()

    null_counts = {}
    for column in df.columns:
        if counts_row[column] > 0:
            null_counts[column] = counts_row[column]
    report["null_counts"] = null_counts

    return df, report


# ===========================================================================
# 3. QUALITY: the job stops before writing anything if the clean data breaks a rule
# ===========================================================================

def run_checks(df):
    """Run every check on the clean Spark table.

    Returns a list of (check name, True or False) pairs. True means the check passed.
    Each count() below makes Spark do the work and give us a number.
    """
    results = []

    # 1. The columns must be exactly the ones the BigQuery table expects
    results.append(("columns match the target schema", df.columns == FINAL_COLUMNS))

    # 2. The table must have at least one row
    results.append(("table is not empty", df.count() > 0))

    # 3. Only one row for each city on each day
    # (group by city and day, then look for any group that has more than one row)
    days_with_copies = df.groupBy("city", "reading_date").count().filter(F.col("count") > 1).count()
    results.append(("one row per city per day", days_with_copies == 0))

    # 4. Every row must have a city and a date
    rows_missing_key = df.filter(F.col("city").isNull() | F.col("reading_date").isNull()).count()
    results.append(("no missing city or date", rows_missing_key == 0))

    # 5. No negative pollution values (empty values are ignored)
    any_negative = F.lit(False)
    for column in POLLUTANTS:
        any_negative = any_negative | (F.col(column) < 0)
    rows_with_negatives = df.filter(any_negative).count()
    results.append(("no negative pollutant values", rows_with_negatives == 0))

    # 6. AQI must be between 0 and 500 (empty values are ignored)
    rows_bad_aqi = df.filter((F.col("aqi") < 0) | (F.col("aqi") > 500)).count()
    results.append(("AQI between 0 and 500", rows_bad_aqi == 0))

    return results


# ===========================================================================
# 4. LOAD: save the results
# ===========================================================================

def write_csv_folder(df, folder_path):
    """Save a Spark DataFrame as CSV (with a header row) into a folder, as ONE part file.

    Spark always writes a folder. coalesce(1) puts everything into one piece, so the folder
    holds a single part-00000-....csv file. The folder is a gs:// path, so Spark writes
    straight into Cloud Storage. mode("overwrite") makes a re-run replace the old files,
    so running the pipeline twice never leaves duplicates.
    coalesce(1) is fine for a small table like this one. For big data you would keep the
    many part files (or write Parquet) so that Spark can write them in parallel.
    """
    df.coalesce(1).write.mode("overwrite").csv(folder_path, header=True)


def write_text_file(spark, text, folder_path):
    """Save a piece of text (for example the JSON report) into a folder, as one part file."""
    one_row = spark.createDataFrame([(text,)], ["value"])
    one_row.coalesce(1).write.mode("overwrite").text(folder_path)


def load_bigquery(df, table, bucket):
    """Replace the BigQuery table with the clean data.

    Dataproc already contains the Spark BigQuery connector, so format("bigquery") just works.
    With the "indirect" method the connector first saves the data as temporary files in
    your bucket, then BigQuery loads those files in one go, and the temporary files are deleted.
    The table is partitioned by day on reading_date and clustered by city.
    mode("overwrite") replaces the table, so a re-run never duplicates rows.
    """
    (df.write.format("bigquery")
        .option("writeMethod", "indirect")
        .option("temporaryGcsBucket", bucket)
        .option("partitionField", "reading_date")
        .option("partitionType", "DAY")
        .option("clusteredFields", "city")
        .mode("overwrite")
        .save(table))


# ===========================================================================
# THE JOB: run the four parts in order
# ===========================================================================

def run_job(spark, bucket, table, input_file):
    """Run the whole pipeline. Returns the data-quality report (a dictionary)."""
    input_path = "gs://" + bucket + "/" + input_file
    processed_folder = "gs://" + bucket + "/processed/daily_air_quality_clean"
    report_folder = "gs://" + bucket + "/reports/quality_report"

    # ----- 1. Extract -----
    print("[1/4] Extract   reading " + input_path)
    raw = read_raw_csv(spark, input_path)

    # ----- 2. Transform -----
    print("[2/4] Transform cleaning and enriching with Spark")
    df, report = clean(raw)

    # ----- 3. Quality gate -----
    print("[3/4] Quality   running data-quality checks")
    results = run_checks(df)

    report["quality_checks"] = []
    failed = []
    for name, passed in results:
        report["quality_checks"].append({"check": name, "passed": passed})
        if not passed:
            failed.append(name)

    # The report is always written, even when a check fails, so you can see what went wrong
    write_text_file(spark, json.dumps(report, indent=2), report_folder)

    print("\n=== DATA QUALITY REPORT ===")
    for key in ["rows_raw", "missing_city_dropped", "invalid_dates_dropped", "duplicates_removed",
                "negative_values_nulled", "empty_rows_dropped", "aqi_derived_from_pm25", "rows_clean"]:
        print(key.ljust(26) + str(report[key]))

    # A failed check stops the job here, so bad data is never written anywhere
    if len(failed) > 0:
        raise ValueError("Data quality checks failed: " + str(failed))

    # ----- 4. Load -----
    print("\n[4/4] Load      writing the clean CSV and loading BigQuery table " + table)
    write_csv_folder(df, processed_folder)
    print("Clean CSV written to " + processed_folder)

    try:
        load_bigquery(df, table, bucket)
    except Exception as error:
        raise RuntimeError(
            "The clean CSV is saved in " + processed_folder + ", but loading BigQuery failed. "
            "Check that the job's service account has the roles BigQuery Data Editor and "
            "BigQuery Job User, and that the dataset exists in the same location as the bucket. "
            "Error: " + str(error))
    print("BigQuery table " + table + " loaded: " + str(report["rows_clean"]) + " rows")

    return report


def main():
    # Read the options typed after the file name (in the console: the Arguments boxes)
    parser = argparse.ArgumentParser(description="Air-quality PySpark job")
    parser.add_argument("--bucket", required=True, help="Cloud Storage bucket name, without gs://")
    parser.add_argument("--table", required=True, help="BigQuery table as PROJECT.DATASET.TABLE")
    parser.add_argument("--input", default="raw/air_quality_raw.csv", help="raw CSV path inside the bucket")
    args = parser.parse_args()

    bucket = args.bucket.replace("gs://", "").strip("/")
    if args.table.count(".") != 2:
        parser.error("--table must look like PROJECT.DATASET.TABLE, for example my-project.air_quality.daily_air_quality")

    spark = get_spark()
    try:
        run_job(spark, bucket, args.table, args.input)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
