This project demonstrates an end-to-end data engineering ETL pipeline using:

- Python
- AWS S3
- AWS Glue (PySpark)
- Amazon Redshift
- Airflow orchestration

Pipeline Steps:
1. Load raw CSV data to S3
2. Transform with AWS Glue PySpark script
3. Store cleaned data in S3 (Parquet)
4. Use Redshift COPY command to load data into data warehouse
5. Automate with Airflow DAG


