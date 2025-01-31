import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType

##############################
####     GLUE SCRIPT    ######
# creates the production table
##############################

args = getResolvedOptions(sys.argv, ["job_name", "ds", 'production_table', 'catalog_name'])
run_date = args['ds']
production_table = args['production_table']
catalog_name = args['catalog_name']

# Initialize SparkSession
spark = (SparkSession.builder
         .config('spark.sql.defaultCatalog', catalog_name)
         .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
         .config(f'spark.sql.catalog.{catalog_name}.catalog-impl','org.apache.iceberg.rest.RESTCatalog')
         .config(f'spark.sql.catalog.{catalog_name}.warehouse',catalog_name)
         .config('spark.sql.catalog.eczachly-academy-warehouse.uri','https://api.tabular.io/ws/')
         .getOrCreate())

glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

spark.sql(f"""
   CREATE TABLE IF NOT EXISTS {production_table} (
    ticker STRING,
    date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT
   )
    USING iceberg
    PARTITIONED BY (date)
""")

job = Job(glueContext)
job.init(args["job_name"], args)