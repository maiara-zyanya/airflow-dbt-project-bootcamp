import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table'])
run_date = args['ds']
output_table = args['output_table']
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

df = spark.sql("SELECT * FROM bootcamp.nba_game_details")
df.sortWithinPartitions("game_id", "team_id").writeTo(output_table) \
    .tableProperty("write.spark.fanout.enabled", "true") \
    .createOrReplace()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

