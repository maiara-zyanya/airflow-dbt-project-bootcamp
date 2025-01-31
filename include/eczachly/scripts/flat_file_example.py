import sys
import ast
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, lit, col, udf, concat, from_json

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table', 'polygon_credentials'])
run_date = args['ds']
output_table = args['output_table']

# Create a SparkSession with S3 endpoint configuration
spark = SparkSession.builder \
    .appName("ReadFromS3") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session
year = run_date.split('-')[0]
month = run_date.split('-')[1]
# Define the S3 bucket and file path
s3_bucket = "s3a://flatfiles"
file_path = f"{s3_bucket}/us_stocks_sip/day_aggs_v1/{year}/{month}/"
polygon_credentials = ast.literal_eval(args['polygon_credentials'])

aws_access_key_id = polygon_credentials['AWS_ACCESS_KEY_ID']
aws_secret_access_key = polygon_credentials['AWS_SECRET_ACCESS_KEY']

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://files.polygon.io/")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

# Read the CSV file from the S3 bucket
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)


spark.sql(f"""
CREATE OR REPLACE TABLE {output_table} (
    ticker STRING,	
    volume BIGINT,
    open DOUBLE,
    close DOUBLE,
    high DOUBLE,
    low DOUBLE,
    window_start BIGINT,
    transactions BIGINT,
    snapshot_date DATE
)
USING iceberg
PARTITIONED BY (snapshot_date)
""")

# Write the DataFrame to the Iceberg table
(
    df.withColumn('snapshot_date', from_unixtime(col("window_start")/lit(1000*1000*1000)).cast('date'))
    .sortWithinPartitions("ticker").writeTo(output_table)
    .tableProperty("write.spark.fanout.enabled", "true")
    .overwritePartitions()
)

# Show the first 5 rows of the DataFrame
df.printSchema()

df.collect()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)