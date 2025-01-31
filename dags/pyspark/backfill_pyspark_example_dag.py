from airflow.decorators import dag
from airflow.utils.dates import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from include.eczachly.glue_job_submission import create_glue_job
from include.eczachly.trino_queries import run_trino_query_dq_check, execute_trino_query

s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
catalog_name = Variable.get("CATALOG_NAME")
aws_region = Variable.get("AWS_GLUE_REGION")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
script_path = "include/eczachly/scripts/backfill_job_example.py"


@dag(
     description="An example PySpark DAG",
     default_args={
         "owner": "Zachary Wilson",
         "start_date": datetime(2024, 5, 1),
         "retries": 1,
     },
     max_active_runs=3,
     # This is a cron interval shortcut
     # * * * * *
     # Shortcuts are
     # @daily
     # @hourly
     # @monthly
     # @yearly
     schedule_interval="@daily",
     catchup=False,
     tags=["pyspark", "glue", "example", "eczachly"],
     template_searchpath='include/eczachly')
def backfill_pyspark_example_dag():
    default_output_table = "zachwilson.prod_table_staging_{{ ds_nodash }}"
    production_table = "zachwilson.prod_table_example"

    run_job = PythonOperator(
        task_id="run_glue_job",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "backfill_pyspark_example_job",
            "script_path": script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Testing Job Spark",
            "arguments": {
                "--ds": "{{ ds }}",
                "--output_table": default_output_table
            },
        },
    )

    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                SELECT 
                    date,
                    COUNT(CASE WHEN some_column IS NULL THEN 1 END) = 0 as column_is_not_null_check,
                    COUNT(1) > 0 AS is_there_data_check
                FROM {default_output_table}
                GROUP BY date
            """
        }
    )

    exchange_step = PythonOperator(
        task_id="exchange_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                   INSERT INTO {production_table}
                   SELECT * FROM {default_output_table}
               """
        }
    )

    cleanup_step = PythonOperator(
        task_id="cleanup_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""DELETE FROM {default_output_table}"""
        }
    )

    run_job >> run_dq_check >> exchange_step >> cleanup_step


backfill_pyspark_example_dag()
