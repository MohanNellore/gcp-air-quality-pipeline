"""Step 4 - ANALYSE: run every .sql file in sql/ and save each result as a CSV."""
from pathlib import Path
from typing import Callable

import pandas as pd

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def list_queries() -> list[Path]:
    return sorted(SQL_DIR.glob("*.sql"))


def render(sql: str, table_ref: str) -> str:
    """The SQL files use {table} so the same query runs locally and in BigQuery."""
    return sql.replace("{table}", table_ref)


def run_all(runner: Callable[[str], pd.DataFrame], table_ref: str, out_dir: str) -> dict[str, pd.DataFrame]:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    results = {}
    for path in list_queries():
        df = runner(render(path.read_text(encoding="utf-8"), table_ref))
        df.to_csv(Path(out_dir) / f"{path.stem}.csv", index=False)
        results[path.stem] = df
    return results
