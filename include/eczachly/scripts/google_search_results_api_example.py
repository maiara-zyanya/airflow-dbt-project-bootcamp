import sys
import requests
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

spark = (SparkSession.builder.getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table'])
run_date = args['ds']
output_table = args['output_table']
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session


api_key = "672d5f906b42593574bd70fe"
url = "https://api.scrapingdog.com/google"
date = datetime.strptime(run_date, "%Y-%m-%d").date()


keywords = [
            'data expert',
            'data expert training',
            'data expert boot camp',
            'data engineering academy',
            'data engineering boot camp',
            'data engineering',
            'zach wilson',
            'zach wilson data',
            'full stack expert',
            'linkedin expert',
            'learn data engineering'
]
countries = {
             'United States': 'us',
             'Canada': 'ca',
             'China': 'cn',
             'Brazil': 'br',
             'Germany': 'de',
             'Spain': 'es',
             'France': 'fr',
             'Japan': 'jp',
             'India': 'in'
             }

search_results = []
for keyword in keywords:
    for (country, code) in countries.items():
        params = {
            "api_key": api_key,
            "query": keyword,
            "results": 100,
            "country": code,
            "page": 0,
            "advance_search": "false"
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            data['keyword'] = keyword
            data['country'] = code
            print(data)
            search_results.append(data)
        else:
            print(f"Request failed with status code: {response.status_code}")

flattened_results = []
for result in search_results:
    organic_results = result['organic_results']
    for organic_result in organic_results:
        organic_result['country'] = result['country']
        organic_result['keyword'] = result['keyword']
        sitelinks = organic_result['extended_sitelinks'] if 'extended_sitelinks' in organic_result else []
        organic_result['extended_sitelinks'] = list(map(lambda x: x['link'], sitelinks))
        flattened_results.append(organic_result)

df = spark.createDataFrame(flattened_results).withColumn('date', lit(date))
query = f"""CREATE TABLE IF NOT EXISTS {output_table} (
        keyword STRING,
        country STRING,
        link STRING,
        title STRING,
        snippet STRING,
        rank INTEGER,
        extended_sitelinks ARRAY<STRING>,
        date DATE
        )
        USING iceberg
        PARTITIONED BY (date)         
        """
spark.sql(query)

(df.select(
    col('keyword'),
    col('country'),
    col('link'),
    col('title'),
    col('snippet'),
    col('rank'),
    col('extended_sitelinks'),
    col("date")
).writeTo(output_table)
 .using("iceberg")
 .partitionedBy("date").overwritePartitions()
 )

job = Job(glueContext)
job.init(args["JOB_NAME"], args)