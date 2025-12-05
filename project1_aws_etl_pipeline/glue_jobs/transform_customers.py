import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['S3_INPUT', 'S3_OUTPUT'])
input_path = args['S3_INPUT']
output_path = args['S3_OUTPUT']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.csv(input_path, header=True, inferSchema=True)

df_clean = df.dropDuplicates() \
             .withColumnRenamed("customer_id", "id") \
             .withColumn("signup_year", df.signup_date.substr(1,4))

df_clean.write.mode("overwrite").parquet(output_path)
