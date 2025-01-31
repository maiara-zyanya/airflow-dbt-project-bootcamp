from airflow.decorators import dag, task
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from include.eczachly.glue_job_submission import create_glue_job
from include.eczachly.trino_queries import run_trino_query_dq_check, execute_trino_query


s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
catalog_name = Variable.get("CATALOG_NAME")
aws_region = Variable.get("AWS_GLUE_REGION")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
script_path = "include/eczachly/scripts/google_search_results_api_example.py"

# Define DAG using @dag decorator
@dag(
    # The name of your DAG and the name of the Python file should match.
    # Both should start with your GitHub username to avoid duplicate DAG names.
    "eczachly_dag",
    description="A simple DAG",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2024, 11, 21),
        "retries": 1,
    },
    schedule_interval="@daily",
    catchup=False,
    tags=["community", 'Zach Wilson'],
    template_searchpath='include/eczachly'
)
def eczachly_dag():
    production_table = 'bootcamp.data_expert_search_results'
    create_production_table = PythonOperator(
        task_id="create_production_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f""" 
            CREATE TABLE IF NOT EXISTS {production_table}  (
                keyword VARCHAR,
                country VARCHAR,
                link VARCHAR,
                title VARCHAR,
                snippet VARCHAR,
                rank INTEGER,
                extended_sitelinks ARRAY<VARCHAR>,
                date DATE
                )
                WITH (
                    format='PARQUET',
                    partitioning = ARRAY['date']
                )      
            """
        }
    )

    default_output_table = 'bootcamp.data_expert_search_results_stg'
    run_extract_job = PythonOperator(
        task_id="run_extract_job",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "google_search_results",
            "script_path": script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "description": "Testing Job Spark",
            "arguments": {
                "--ds": "{{ ds }}", # 2024-11-21
                "--output_table": default_output_table
            },
        },
    )
    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': """
                       SELECT 
                           country,
                           keyword,
                           COALESCE(COUNT(CASE WHEN link LIKE '%dataexpert.io%' THEN 1 END), 0) > 0 
                                        AS dataexpert_in_top_100,  
                           COUNT(1) = 100 
                                        AS one_hundred_rows_per_country
                       FROM {default_output_table} 
                       WHERE date = DATE('{ds}')
                       GROUP BY country, keyword
                   """.format(default_output_table=default_output_table, ds='{{ ds }}')
        }
    )
    exchange_stage_data = PythonOperator(
        task_id="exchange_stage_data",
        python_callable=execute_trino_query,
        op_kwargs={
            'query':  """
                           INSERT INTO {production_table}
                           SELECT 
                               *
                           FROM {default_output_table} 
                           WHERE date = DATE('{ds}')
                       """.format(production_table=production_table,
                                  default_output_table=default_output_table,
                                  ds='{{ ds }}')
        }
    )
    create_production_table >> run_extract_job >> run_dq_check >> exchange_stage_data



eczachly_dag()
