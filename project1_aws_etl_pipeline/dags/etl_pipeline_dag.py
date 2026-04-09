from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from datetime import datetime

with DAG(
    dag_id="aws_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "aws", "portfolio"]
):

    run_glue = GlueJobOperator(
        task_id="transform_customers",
        job_name="demo-glue-transform-customers",
        script_location="s3://demo-etl-pipeline-bucket/scripts/transform_customers.py",
        region_name="us-east-1",
        iam_role_name="AWSGlueServiceRole-Demo",
        create_job_kwargs={
            "GlueVersion": "4.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X"
        }
    )

    load_to_redshift = RedshiftSQLOperator(
        task_id="load_to_redshift",
        sql="""
        COPY public.customers
        FROM 's3://demo-etl-pipeline-bucket/clean/customers/'
        IAM_ROLE 'arn:aws:iam::113570284336:role/RedshiftS3AccessRoleDemo'
        FORMAT AS PARQUET;
        """,
        redshift_conn_id="redshift_default"
    )

    run_glue >> load_to_redshift