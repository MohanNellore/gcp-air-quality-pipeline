"""Check that the Spark pipeline cleaned the data exactly as expected.

  python scripts/compare_with_expected.py

Run it after:  python run_pipeline.py   (with the default file data/raw/air_quality_raw.csv)
It compares outputs/daily_air_quality_clean.csv and outputs/quality_report.json with the reference
results saved in tests/expected/. It prints OK if everything matches.
"""
import csv
import json
import os
import sys

project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPECTED_CSV = os.path.join(project_folder, "tests", "expected", "expected_clean_output.csv")

# The numbers expected for data/raw/air_quality_raw.csv
EXPECTED_REPORT = {
    "rows_raw": 4445,
    "missing_city_dropped": 0,
    "invalid_dates_dropped": 21,
    "negative_values_nulled": 44,
    "duplicates_removed": 65,
    "empty_rows_dropped": 87,
    "aqi_derived_from_pm25": 212,
    "rows_clean": 4272,
    "cities": ["Bengaluru", "Chennai", "Delhi", "Hyderabad", "Kolkata", "Mumbai"],
    "date_range": ["2022-01-01", "2023-12-31"],
    "null_counts": {"pm25": 126, "pm10": 43, "aqi": 2, "aqi_bucket": 2, "aqi_source": 2},
}


def read_csv_rows(path):
    """Read a CSV file with Python's csv module. Returns (header, list of rows)."""
    with open(path, newline="", encoding="utf-8") as file:
        rows = list(csv.reader(file))
    return rows[0], rows[1:]


def find_report_differences(report):
    """Compare a quality report (a dictionary) with the expected one. Returns a list of messages."""
    messages = []
    for key, expected_value in EXPECTED_REPORT.items():
        if report.get(key) != expected_value:
            messages.append(key + ": expected " + str(expected_value) + ", got " + str(report.get(key)))
    return messages


def find_differences(new_path, expected_path=EXPECTED_CSV):
    """Compare two CSV files cell by cell. Returns a list of messages (empty means they match).

    Numbers are compared as numbers (27.5 equals 27.50).
    """
    new_header, new_rows = read_csv_rows(new_path)
    old_header, old_rows = read_csv_rows(expected_path)

    if new_header != old_header:
        return ["The column names differ: " + str(new_header)]
    if len(new_rows) != len(old_rows):
        return ["The number of rows differs: " + str(len(new_rows)) + " instead of " + str(len(old_rows))]

    messages = []
    for row_number, (new_row, old_row) in enumerate(zip(new_rows, old_rows), start=2):
        for column, new_cell, old_cell in zip(new_header, new_row, old_row):
            if new_cell == old_cell:
                continue
            try:
                same_number = abs(float(new_cell) - float(old_cell)) < 1e-9
            except ValueError:
                same_number = False
            if not same_number:
                messages.append("line " + str(row_number) + ", column " + column + ": got '"
                                + new_cell + "', expected '" + old_cell + "'")
    return messages


def main():
    out_folder = os.path.join(project_folder, "outputs")
    csv_path = os.path.join(out_folder, "daily_air_quality_clean.csv")
    report_path = os.path.join(out_folder, "quality_report.json")

    for path in (csv_path, report_path):
        if not os.path.exists(path):
            print("Missing " + path + ". Run the pipeline first: python run_pipeline.py")
            sys.exit(1)

    with open(report_path, encoding="utf-8") as file:
        report = json.load(file)

    problems = find_report_differences(report) + find_differences(csv_path)

    if problems:
        print(str(len(problems)) + " difference(s) from the expected output. The first ones:")
        for message in problems[:10]:
            print("  " + message)
        sys.exit(1)

    print("OK: the report and all " + str(report["rows_clean"]) + " rows match the expected output.")


if __name__ == "__main__":
    main()
