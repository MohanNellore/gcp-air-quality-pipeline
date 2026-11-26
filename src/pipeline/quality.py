"""Data-quality gate: the pipeline stops before loading if the clean data breaks a rule."""
from pyspark.sql import functions as F

from .rules import FINAL_COLUMNS, POLLUTANTS


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


def assert_quality(df):
    """Run the checks. If any check fails, stop the pipeline with an error."""
    results = run_checks(df)

    failed = []
    for name, passed in results:
        if not passed:
            failed.append(name)

    if len(failed) > 0:
        raise ValueError("Data quality checks failed: " + str(failed))

    return results
