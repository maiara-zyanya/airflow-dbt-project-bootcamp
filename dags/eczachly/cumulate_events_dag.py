from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.eczachly.poke_tabular_partition import poke_tabular_partition
from include.eczachly.trino_queries import execute_trino_query
import os
from airflow.models import Variable
local_script_path = os.path.join("include", 'eczachly/scripts/kafka_read_example.py')
tabular_credential = Variable.get("TABULAR_CREDENTIAL")

# TODO make sure to rename this if you're testing this dag out!
schema = 'zachwilson'
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
def cumulative_web_events_dag():

    upstream_table = f'{schema}.user_web_events_daily'
    production_table = f'{schema}.user_web_events_cumulated'
    wait_for_web_events = PythonOperator(
        task_id='wait_for_web_events',
        depends_on_past=True,
        python_callable=poke_tabular_partition,
        op_kwargs={
            "tabular_credential": tabular_credential,
            "table": upstream_table,
            "partition": 'ds={{ ds }}'
        },
        provide_context=True  # This allows you to pass additional context to the function
    )

    create_step = PythonOperator(
        task_id="create_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
             CREATE TABLE IF NOT EXISTS {production_table} (
                user_id INTEGER,
                academy_id INTEGER,
                event_count_array ARRAY(INTEGER),
                event_count_last_7d INTEGER,
                event_count_lifetime INTEGER,
                ds DATE
             ) WITH (
                format = 'PARQUET',
                partitioning = ARRAY['day(ds)']
             )
             """
        }
    )

    yesterday_ds = '{{ yesterday_ds }}'
    ds = '{{ ds }}'
    clear_step = PythonOperator(
        task_id="clear_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
               DELETE FROM {production_table} 
               WHERE ds = DATE('{ds}')
               """
        }
    )

    cumulate_step = PythonOperator(
        task_id="cumulate_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                 INSERT INTO {production_table}
                 WITH yesterday AS (
                    SELECT * FROM {production_table}
                    WHERE ds = DATE('{ yesterday_ds }')
                    AND academy_id = 2
                 ),
                 today AS (
                    SELECT user_id, academy_id, MAX(event_count) as event_count
                    FROM {upstream_table}
                    WHERE ds = DATE('{ds}')
                    AND academy_id = 2
                    GROUP BY user_id, academy_id
                 ),
                 event_arrays AS (
                 SELECT 
                        COALESCE(t.user_id, y.user_id) as user_id,
                        COALESCE(t.academy_id, y.academy_id) as academy_id,
                        CASE 
                            WHEN y.user_id IS NULL THEN ARRAY[t.event_count]
                            WHEN t.user_id IS NULL THEN ARRAY[0] || y.event_count_array
                            ELSE ARRAY[t.event_count] || y.event_count_array
                        END as event_count_array,
                        COALESCE(y.event_count_lifetime,0) as event_count_lifetime
                    FROM today t 
                    FULL OUTER JOIN yesterday y ON t.user_id = y.user_id 
                ) 
                
                SELECT user_id, 
                        academy_id, 
                        event_count_array,
                        reduce(
                            slice(event_count_array, 1, 7), 
                            0, 
                            (acc, x) -> acc + coalesce(x, 0),
                            acc -> acc
                        ) AS event_count_last_7d, as  event_count_last_7d,
                        event_count_lifetime + ELEMENT_AT(event_count_array, 1)  as event_count_lifetime,
                        DATE('{ds}') as ds 
                FROM event_arrays   
                 """
        }
    )

    wait_for_web_events >> create_step >> clear_step >> cumulate_step


cumulative_web_events_dag()
