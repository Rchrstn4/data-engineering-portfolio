from extract import extract_sales_data
from transform import transform_sales_data
from load import load_to_sqlite


RAW_FILE = "data/raw_sales.csv"
DB_PATH = "output/etl_demo.db"
TABLE_NAME = "daily_sales_summary"


def run_etl() -> None:
    print("Starting ETL process...")

    raw_df = extract_sales_data(RAW_FILE)
    print(f"Extracted {len(raw_df)} rows.")

    transformed_df = transform_sales_data(raw_df)
    print(f"Transformed into {len(transformed_df)} summary rows.")

    load_to_sqlite(transformed_df, DB_PATH, TABLE_NAME)
    print(f"Loaded data into {DB_PATH}, table: {TABLE_NAME}")

    print("ETL process completed successfully.")


if __name__ == "__main__":
    run_etl()