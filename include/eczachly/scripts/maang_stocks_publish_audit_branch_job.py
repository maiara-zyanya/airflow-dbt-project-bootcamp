import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType

##################################
####     GLUE SCRIPT        ######
# Run DQ tests on the audit branch
##################################

args = getResolvedOptions(sys.argv, ["job_name", "ds", 'production_table', 'catalog_name'])
run_date = args['ds']
production_table = args['production_table']
catalog_name = args['catalog_name']
branch_name = 'audit_branch'

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

query = f"""
    SELECT snapshot_id
    FROM {production_table}.refs
    WHERE name = '{branch_name}'
"""
wap_snapshot_id = spark.sql(query).head().snapshot_id
publish_query = f"CALL system.cherrypick_snapshot({production_table}, {wap_snapshot_id})"

job = Job(glueContext)
job.init(args["job_name"], args)