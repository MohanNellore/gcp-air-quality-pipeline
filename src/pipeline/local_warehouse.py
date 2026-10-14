"""Local stand-in for BigQuery so the whole project runs (and is tested) with no cloud account.

SQLite ships with Python. The SQL files in sql/ use only syntax that SQLite and BigQuery share.
"""
import sqlite3

import pandas as pd


def load_dataframe(df: pd.DataFrame, table: str, db_path: str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    df.to_sql(table, conn, if_exists="replace", index=False)
    return conn


def run_query(conn: sqlite3.Connection, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn)
