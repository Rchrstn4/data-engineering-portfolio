import boto3
import pandas as pd
import io

def upload_to_s3(bucket, key, csv_path):
    s3 = boto3.client('s3')

    df = pd.read_csv(csv_path)

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue()
    )

if __name__ == "__main__":
    upload_to_s3(
        bucket="my-etl-bucket",
        key="raw/customers/customers.csv",
        csv_path="customers.csv"
    )
