import sqlite3
import pandas as pd


def load_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str) -> None:
    """Load transformed data into SQLite."""
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    finally:
        conn.close()