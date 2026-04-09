import boto3
import pandas as pd
import io


def upload_to_s3(bucket: str, key: str, csv_path: str) -> None:
    """Upload a local CSV file to S3."""
    
    s3 = boto3.client("s3")

    # Read local file
    df = pd.read_csv(csv_path)

    # Convert to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue()
    )

    print(f"Uploaded {csv_path} to s3://{bucket}/{key}")


if __name__ == "__main__":
    upload_to_s3(
        bucket="demo-etl-pipeline-bucket",
        key="raw/customers/customers.csv",
        csv_path="data/customers.csv"
    )