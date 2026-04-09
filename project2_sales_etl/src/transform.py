import pandas as pd


def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform raw sales data into daily product sales summary."""
    df = df.copy()

    df["order_date"] = pd.to_datetime(df["order_date"])
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)

    df["sales_amount"] = df["quantity"] * df["price"]

    summary_df = (
        df.groupby(["order_date", "category", "product"], as_index=False)
        .agg(
            total_quantity=("quantity", "sum"),
            total_sales=("sales_amount", "sum"),
            total_orders=("order_id", "count"),
            unique_customers=("customer_id", "nunique"),
        )
    )

    return summary_df