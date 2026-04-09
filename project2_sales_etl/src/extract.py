import pandas as pd


def extract_sales_data(file_path: str) -> pd.DataFrame:
    """Extract raw sales data from CSV."""
    df = pd.read_csv(file_path)
    return df