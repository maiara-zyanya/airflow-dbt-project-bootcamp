from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.eczachly.poke_tabular_partition import poke_tabular_partition
from include.eczachly.trino_queries import execute_trino_query
import os
from airflow.models import Variable

local_script_path = os.path.join("include", 'eczachly/scripts/kafka_read_example.py')
tabular_credential = Variable.get("TABULAR_CREDENTIAL")


# This DAG is not idempotent. It is missing so many things
# Partition sensors
# Bad query logic
# Deletes
@dag(
    description="A dag that aggregates data from Iceberg into metrics",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2024, 10, 18),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2024, 10, 18),
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=True,
    template_searchpath='include/eczachly',
    tags=["community"],
)
def non_idempotent_dag():
    # TODO make sure to rename this if you're testing this dag out!
    upstream_table = 'bootcamp.user_web_events_daily'
    summary_table = 'bootcamp.academy_web_events_summary_monthly'
    ds = '{{ ds }}'
    thirty_days_ago = "{{ macros.ds_add(ds, -30) }}"

    academy_summary_create = PythonOperator(
        task_id="academy_summary_create",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {summary_table} (
                   academy_id INTEGER,
                   monthly_active_users INTEGER,
                   ds DATE
                ) WITH (
                   format = 'PARQUET',
                   partitioning = ARRAY['day(ds)']
                )
                """
        }
    )
    insert_academy_summary = PythonOperator(
        task_id="insert_academy_summary",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                    INSERT INTO {summary_table}  
                    SELECT 
                        academy_id, 
                        COUNT(DISTINCT user_id) as monthly_active_users,
                        DATE('{ds}') as ds
                    FROM {upstream_table}
                    WHERE ds > DATE('{thirty_days_ago}')
                    GROUP BY academy_id
                    """
        }
    )
    academy_summary_create >> insert_academy_summary


non_idempotent_dag()