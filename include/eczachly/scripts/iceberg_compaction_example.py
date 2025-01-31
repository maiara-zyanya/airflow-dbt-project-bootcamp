import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

# Initialize Spark session
spark = (SparkSession.builder
         .getOrCreate())

# Get job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table'])
run_date = args['ds']
output_table = args['output_table']
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Uncomment for compaction
# spark.sql(f""" CALL system.rewrite_data_files( table => '{output_table}',
#             strategy => 'sort',
#             sort_order => 'zorder(name)',
#             options => map('rewrite-all', 'true') )
#         """)

spark.sql("""
 CALL system.rewrite_data_files(table => 'prachisharma4326.test_compaction_v2',
        strategy => 'sort',
        sort_order => 'zorder(name)',
        options => map('rewrite-all', 'true') )
""")

# Uncomment for fast-forwarding
spark.sql(f""" CALL system.fast_forward('{output_table}', 'main', 'audit_branch') """)

# Initialize and run Glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
