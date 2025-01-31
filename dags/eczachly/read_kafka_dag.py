from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.eczachly.glue_job_submission import create_glue_job
import os
from airflow.models import Variable
local_script_path = os.path.join("include", 'eczachly/scripts/kafka_read_example.py')

s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
catalog_name = Variable.get("CATALOG_NAME")
aws_region = Variable.get("AWS_GLUE_REGION")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
kafka_credentials = Variable.get("KAFKA_CREDENTIALS")
@dag(
    description="A dag that reads from the Kafka queue and dumps the data to Iceberg",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2024, 5, 1),
        "retries": 1,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2024, 10, 1),
    max_active_runs=15,
    schedule_interval="@daily",
    catchup=True,
    template_searchpath='include/eczachly',
    tags=["community"],
)
def read_kafka_dag():
    start_glue_job_task = PythonOperator(
        task_id='start_glue_job',
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "read_web_events_kafka_{{ ds }}",
            "script_path": local_script_path,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "tabular_credential": tabular_credential,
            "s3_bucket": s3_bucket,
            "catalog_name": catalog_name,
            "aws_region": aws_region,
            "kafka_credentials": kafka_credentials,
            "description": "Testing Job Spark",
            "arguments": {
                "--ds": "{{ ds }}",
                "--output_table": 'bootcamp.web_events_production'
            },
        },
        provide_context=True  # This allows you to pass additional context to the function
    )

read_kafka_dag()
