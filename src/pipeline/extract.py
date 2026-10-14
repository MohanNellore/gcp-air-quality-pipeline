"""Step 1 - EXTRACT: read the raw CSV exactly as it arrived (no cleaning here)."""
from pathlib import Path

import pandas as pd


def read_raw_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Raw file not found: {path}")
    df = pd.read_csv(p, dtype=str)  # read everything as text; typing is done in transform
    if df.empty:
        raise ValueError(f"Raw file is empty: {path}")
    return df
