"""Data-quality gate: the pipeline stops before loading if the cleaned data breaks a rule."""
import pandas as pd

from .transform import FINAL_COLUMNS


def run_checks(df: pd.DataFrame) -> list[tuple[str, bool]]:
    return [
        ("columns match the target schema", list(df.columns) == FINAL_COLUMNS),
        ("table is not empty", len(df) > 0),
        ("one row per city per day", not df.duplicated(subset=["city", "reading_date"]).any()),
        ("no missing city or date", bool(df["city"].notna().all() and df["reading_date"].notna().all())),
        ("no negative pollutant values", bool((df[["pm25", "pm10", "no2", "so2", "co", "o3"]].fillna(0) >= 0).all().all())),
        ("AQI between 0 and 500", bool(df["aqi"].dropna().between(0, 500).all())),
    ]


def assert_quality(df: pd.DataFrame) -> list[tuple[str, bool]]:
    results = run_checks(df)
    failed = [name for name, ok in results if not ok]
    if failed:
        raise ValueError(f"Data quality checks failed: {failed}")
    return results
