from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import datetime, timedelta
from airflow.models import Variable
from include.eczachly.aws_secret_manager import get_secret
from include.eczachly.glue_job_submission import create_glue_job
from include.eczachly.trino_queries import execute_trino_query
from datetime import datetime
import pandas_market_calendars as mcal
import requests
import ast


s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
catalog_name = Variable.get("CATALOG_NAME")  #'eczachly-academy-warehouse'
aws_region = Variable.get("AWS_GLUE_REGION")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
polygon_api_key = ast.literal_eval(get_secret("POLYGON_CREDENTIALS"))['AWS_SECRET_ACCESS_KEY']
schema = 'prachisharma4326'
branch_name = 'audit_branch'

###################
##  GLUE SCRIPTS ##
###################
create_daily_step_script_path = 'include/eczachly/scripts/maang_stocks_create_production_table_job.py'
create_branch_step_script_path = 'include/eczachly/scripts/maang_stocks_create_staging_branch_job.py'
run_dq_check_step_script_path = 'include/eczachly/scripts/maang_stocks_run_dq_test_job.py'
publish_audit_branch_script_path = 'include/eczachly/scripts/maang_stocks_publish_audit_branch_job.py'

@dag(
    description="PySpark DAG for loading and managing data in Iceberg",
    default_args={
        "owner": schema,
        "start_date": datetime(2025, 1, 10),
        "retries": 1,
        "execution_timeout": timedelta(hours=1),
    },
    schedule_interval="@daily",
    catchup=True,
    tags=["pyspark", "iceberg"],
)

def pyspark_iceberg_dag():
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']
    production_table = f'{schema}.maang_stock_prices_pyspark'
    cumulative_table = f'{schema}.maang_stock_prices_cummulated'
    ds = '{{ ds }}'

    def is_market_day(ds):
        #The is_market_day function checks whether the provided date is in the list of market days.
        nyse = mcal.get_calendar('NYSE')
        #This returns a list of all valid dates (non-holidays) withing the start and end dates. 
        market_days = nyse.valid_days(start_date=ds, end_date=ds)
        #return a boolean value (True or False) for when market days is greater than 1
        return ds in market_days
        
    #The ShortCircuitOperator in Apache Airflow is simple but powerful. It allows skipping tasks based on the result of a condition.
    check_market_day = ShortCircuitOperator(
        task_id='check_market_day',
        python_callable=is_market_day,
        provide_context=True
    )

    # TODO create schema for daily stock price summary table
    create_daily_step = PythonOperator(
        task_id="create_daily_step", 
        depends_on_past=True,
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "maang_stocks_create_daily_step",
            "script_path": create_daily_step_script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Spark Job to Create an Iceberg table if it does not exist",
            "arguments": {
                "--job_name": "maang_stocks_create_daily_step",
                "--ds": "{{ ds }}",
                "--production_table": production_table,
                "--catalog_name": catalog_name
            },
        }
    )

    # todo make sure you load into the staging table not production
    create_branch_step = PythonOperator(
        task_id="create_branch_step", 
        depends_on_past=True,
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "maang_stocks_create_branch_step",
            "script_path": create_branch_step_script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Spark Job to Create an branch on Iceberg table and load data",
            "arguments": {
                "--job_name": "maang_stocks_create_branch_step",
                "--ds": "{{ ds }}",
                "--production_table": production_table,
                "--catalog_name": catalog_name,
                "--polygon_api_key": polygon_api_key
            },
        }
    )

    # TODO figure out some nice data quality checks
    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        depends_on_past=True,
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "maang_stocks_run_dq_check_step",
            "script_path": run_dq_check_step_script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Spark Job to run DQ step on Iceberg table branch",
            "arguments": {
                "--job_name": "maang_stocks_run_dq_check_step",
                "--ds": "{{ ds }}",
                "--production_table": production_table,
                "--catalog_name": catalog_name
            },
        }
    )

    # TODO figure out some nice data quality checks
    publish_audit_branch = PythonOperator(
        task_id="publish_audit_branch",
        depends_on_past=True,
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "maang_stocks_publish_audit_branch",
            "script_path": publish_audit_branch_script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Spark Job to publish audit branch to main",
            "arguments": {
                "--job_name": "maang_stocks_publish_audit_branch",
                "--ds": "{{ ds }}",
                "--production_table": production_table,
                "--catalog_name": catalog_name
            },
        }
    )

    # TODO figure out the right dependency chain
    check_market_day >> create_daily_step >> create_branch_step >> run_dq_check >> publish_audit_branch

pyspark_iceberg_dag()