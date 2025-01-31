import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, udf, concat, from_json
from pyspark.sql.types import StringType,StructType,StructField,IntegerType,DoubleType, LongType

import requests
spark = (SparkSession.builder.getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table'])
run_date = args['ds']
output_table = args['output_table']

glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

tickers = spark.sql(f"""SELECT * FROM bootcamp.stock_tickers 
    WHERE date = DATE('{run_date}')
""")


def query_api(ticker, date):
    url = f'https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&apiKey=Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q'
    try:
        response = requests.get(url)
        return response.json()
    except Exception as e:
        print(e)
        return None

schema = StructType(
    [
        StructField("from", StringType()),
        StructField("symbol", StringType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", LongType()),
        StructField("afterHours", DoubleType()),
        StructField("preMarket", DoubleType()),
     ]
)
# Register the function as a UDF
query_api_udf = udf(lambda ticker, date: query_api(ticker, date), schema)

spark.sql("""CREATE TABLE IF NOT EXISTS bootcamp.stock_prices (
    from string,
    symbol string,
    open DOUBLE,
    high double,
    low double,
    close double,
    volume BIGINT,
    afterHours double,
    preMarket DOUBLE
    )
    USING iceberg 
    PARTITIONED BY (from)
""")


all_data = tickers.repartition(4).withColumn("daily_prices", query_api_udf(col("ticker"), lit(run_date)))
all_data.select(
    col("daily_prices.from").alias("from"),
    col("daily_prices.symbol").alias("symbol"),
    col("daily_prices.open").alias("open"),
    col("daily_prices.high").alias("high"),
    col("daily_prices.low").alias("low"),
    col("daily_prices.close").alias("close"),
    col("daily_prices.volume").alias("volume"),
    col("daily_prices.afterHours").alias("afterHours"),
    col("daily_prices.preMarket").alias("preMarket"),
).where(col("high").isNotNull()).writeTo('bootcamp.stock_prices').using("iceberg").partitionedBy("from").overwritePartitions()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)




