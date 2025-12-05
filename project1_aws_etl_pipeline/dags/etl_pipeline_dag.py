from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from datetime import datetime

with DAG(
    "aws_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
):

    run_glue = GlueJobOperator(
        task_id="transform_customers",
        job_name="glue-transform-customers",
        script_location="s3://my-etl-bucket/scripts/transform_customers.py",
        region_name="us-east-1"
    )

    load_to_redshift = RedshiftSQLOperator(
        task_id="load_to_redshift",
        sql="""
        COPY customers
        FROM 's3://my-etl-bucket/clean/customers/'
        IAM_ROLE 'arn:aws:iam::YOUR_NUMBER:role/MyRedshiftRole'
        FORMAT AS PARQUET;
        """
    )

    run_glue >> load_to_redshift
