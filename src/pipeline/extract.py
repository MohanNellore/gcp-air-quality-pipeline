"""Step 1 - EXTRACT: read the raw CSV file exactly as it arrived (no cleaning here)."""
import os

import pandas as pd


def read_raw_csv(path):
    """Read the CSV file and return it as a table (a pandas DataFrame)."""
    if not os.path.exists(path):
        raise FileNotFoundError("Raw file not found: " + path)

    # dtype=str means: read every column as plain text.
    # We do not let pandas guess the types because it can guess wrong.
    # Changing text into numbers and dates is done later, in transform.py.
    df = pd.read_csv(path, dtype=str)

    if df.empty:
        raise ValueError("Raw file is empty: " + path)

    return df
