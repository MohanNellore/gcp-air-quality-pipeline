"""Data-quality gate: the pipeline stops before loading if the clean data breaks a rule."""
from .transform import FINAL_COLUMNS, POLLUTANTS


def run_checks(df):
    """Run every check on the clean table.

    Returns a list of (check name, True or False) pairs. True means the check passed.
    """
    results = []

    # 1. The columns must be exactly the ones the BigQuery table expects
    columns_ok = list(df.columns) == FINAL_COLUMNS
    results.append(("columns match the target schema", columns_ok))

    # 2. The table must have at least one row
    results.append(("table is not empty", len(df) > 0))

    # 3. Only one row for each city on each day
    has_duplicates = df.duplicated(subset=["city", "reading_date"]).any()
    results.append(("one row per city per day", not has_duplicates))

    # 4. Every row must have a city and a date
    all_present = df["city"].notna().all() and df["reading_date"].notna().all()
    results.append(("no missing city or date", bool(all_present)))

    # 5. No negative pollution values (empty values are ignored)
    pollution = df[POLLUTANTS].fillna(0)
    no_negatives = (pollution >= 0).all().all()
    results.append(("no negative pollutant values", bool(no_negatives)))

    # 6. AQI must be between 0 and 500 (empty values are ignored)
    aqi_values = df["aqi"].dropna()
    aqi_ok = aqi_values.between(0, 500).all()
    results.append(("AQI between 0 and 500", bool(aqi_ok)))

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
