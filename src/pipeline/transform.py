"""Step 2 - TRANSFORM: clean the data and add useful new columns, using PySpark.

Two things to know about Spark:
  * A Spark DataFrame is like a table that is split into pieces, so Spark can work on the pieces in parallel.
  * Spark is LAZY. Lines like filter() and withColumn() only write down a plan.
    Spark does the real work when we ask for a result with an ACTION such as count(), collect() or write.

The main function is clean(). It fixes the data one rule at a time and counts
how many rows each rule changed. Those counts become the data-quality report.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from .rules import (
    AQI_BUCKETS,
    CITY_ALIASES,
    FINAL_COLUMNS,
    OTHER_SEASON,
    PM25_BREAKPOINTS,
    POLLUTANTS,
    SEASON_MONTHS,
)


# ---------------------------------------------------------------------------
# Column helpers. Each one takes a Spark column and returns a new Spark column.
# They are the Spark versions of the small functions in rules.py.
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

    # Keep only the final columns, in the right order, sorted by city and date.
    # cache() so the checks and the file writing below do not repeat all the work above.
    df = df.select(FINAL_COLUMNS).orderBy("city", "reading_date").cache()

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
