import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType
import requests

################################################
####     GLUE SCRIPT                      ######
# creates the iceberg branch on production table
################################################

args = getResolvedOptions(sys.argv, ["job_name", "ds", 'production_table', 'catalog_name','polygon_api_key'])
run_date = args['ds']
production_table = args['production_table']
catalog_name = args['catalog_name']
polygon_api_key = args['polygon_api_key']
branch_name = 'audit_branch'
print (branch_name)

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

#Create an Iceberg branch if it does not already exist.
def create_branch_if_not_exists(table_name, branch_name):
    try:
        spark.sql(f"ALTER TABLE {production_table} CREATE BRANCH {branch_name}")
        print(f"Branch '{branch_name}' created successfully.")
    except Exception as e:
        if "already exists" in str(e):
            print(f"Branch '{branch_name}' already exists. Skipping creation.")
        else:
            raise e

def load_data_to_iceberg():
    #Load stock data from Polygon API to Iceberg staging branch.
    create_branch_if_not_exists(production_table, branch_name)
    # Before writing to the table we set spark.wap.branch so that writes (and reads) are against the specified branch of the table.
    spark.conf.set('spark.wap.branch', branch_name)
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']
    rows = []
    for ticker in maang_stocks:
        polygon_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{run_date}/{run_date}?adjusted=true&sort=asc&apiKey={polygon_api_key}"
        response = requests.get(polygon_url)
        if response.status_code == 200:
            results = response.json().get("results", [])
            for result in results:
                rows.append((
                    ticker,
                    run_date,
                    float(result.get("o", 0.0)),  # Open price
                    float(result.get("h", 0.0)),  # High price
                    float(result.get("l", 0.0)),  # Low price
                    float(result.get("c", 0.0)),  # Close price
                    int(result.get("v", 0)),      # Volume (integer)
                ))
        else:
            raise ValueError(f"Failed to fetch data for {ticker}: {response.status_code}")

    schema = StructType([
        StructField("ticker", StringType(), True),
        StructField("date", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
    ])

    df = spark.createDataFrame(rows, schema=schema)
    df = df.withColumn("date", df["date"].cast(DateType()))

    # Overwrite to Iceberg staging branch
    df.writeTo(f"{production_table}.branch_{branch_name}") \
        .using("iceberg") \
        .partitionedBy("date") \
        .overwritePartitions()

    spark.conf.unset('spark.wap.branch')

load_data_to_iceberg()

#run DQ tests on the new branch


job = Job(glueContext)
job.init(args["job_name"], args)