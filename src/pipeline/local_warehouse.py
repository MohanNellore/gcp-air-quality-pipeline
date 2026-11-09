"""A small local stand-in for the BigQuery load step, so the project runs with no cloud account.

SQLite comes with Python, so nothing extra needs to be installed.
"""
import sqlite3


def load_dataframe(df, table, db_path=":memory:"):
    """Save the table into a SQLite database and return the connection.

    ":memory:" means the database lives in memory only and disappears when the program ends.
    """
    connection = sqlite3.connect(db_path)
    df.to_sql(table, connection, if_exists="replace", index=False)
    return connection
