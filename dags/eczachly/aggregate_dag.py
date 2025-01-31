from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.eczachly.poke_tabular_partition import poke_tabular_partition
from include.eczachly.trino_queries import run_trino_query_dq_check, execute_trino_query

# TODO MAKE SURE TO CHANGE THIS TO YOUR USERNAME
SCHEMA = 'zachwilson'
import os
from airflow.models import Variable
local_script_path = os.path.join("include", 'eczachly/scripts/kafka_read_example.py')
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
@dag(
    description="A dag that aggregates data from Iceberg into metrics",
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
def aggregate_kafka_events_dag():
    upstream_table = 'bootcamp.web_events_production'
    wait_for_web_events = PythonOperator(
        task_id='wait_for_web_events',
        python_callable=poke_tabular_partition,
        op_kwargs={
            "tabular_credential": tabular_credential,
            "table": upstream_table,
            "partition": 'event_time_day={{ ds }}'
        },
        provide_context=True  # This allows you to pass additional context to the function
    )
    production_table = f'{SCHEMA}.user_web_events_daily'
    create_step = PythonOperator(
        task_id="create_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
             CREATE TABLE IF NOT EXISTS {production_table} (
                user_id INTEGER,
                academy_id INTEGER,
                lesson_page_count INTEGER,
                event_count INTEGER, 
                ds DATE    
             ) WITH (
                format = 'PARQUET',
                partitioning = ARRAY['ds']
             )
             """
        }
    )
    staging_table = production_table + '_stg_{{ ds_nodash }}'
    create_staging_step = PythonOperator(
        task_id="create_staging_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                 CREATE TABLE IF NOT EXISTS {staging_table} (
                    user_id INTEGER,
                    academy_id INTEGER,
                    lesson_page_count INTEGER,
                    event_count INTEGER, 
                    ds DATE    
                 ) WITH (
                    format = 'PARQUET',
                    partitioning = ARRAY['ds']
                 )
                 """
        }
    )

    clear_production_table = PythonOperator(
        task_id="clear_production_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': """
                        DELETE FROM {production_table}
                        WHERE ds = DATE('{ds}')
                    """.format(production_table=production_table, ds='{{ ds }}')
        }
    )


    load_to_staging_step = PythonOperator(
        task_id="load_to_staging_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': """
                INSERT INTO {staging_table}
                SELECT 
                    user_id,
                    academy_id, 
                    COUNT(CASE WHEN url LIKE '%lesson%' THEN 1 END) as lesson_count,
                    COUNT(1) AS event_count,
                    DATE('{ds}') as ds 
                FROM {upstream_table}
                WHERE user_id IS NOT NULL
                AND event_time BETWEEN DATE('{yesterday_ds}') AND DATE('{ds}')
                GROUP BY user_id, academy_id 
                """.format(staging_table=staging_table,
                           upstream_table=upstream_table,
                           yesterday_ds='{{ yesterday_ds }}',
                           ds='{{ ds }}')
        }

    )

    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                   SELECT 
                       user_id,
                       COUNT(CASE WHEN event_count = 0 THEN 1 END) = 0 as event_count_should_not_be_zero,
                       COUNT(1) < 5000 AS is_there_too_much_data
                   FROM {staging_table}
                   GROUP BY user_id
               """
        }
    )

    exchange_data_from_staging = PythonOperator(
        task_id="exchange_data_from_staging",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': """
                          INSERT INTO {production_table}
                          SELECT * FROM {staging_table} 
                          WHERE ds = DATE('{ds}')
                      """.format(production_table=production_table,
                                 staging_table=staging_table,
                                 ds='{{ ds }}')
        }
    )

    drop_staging_table = PythonOperator(
        task_id="drop_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': """
                             DROP TABLE {staging_table}
                         """.format(staging_table=staging_table)
        }
    )

    (wait_for_web_events
     >> create_step
     >> create_staging_step
     >> clear_production_table
     >> load_to_staging_step
     >> run_dq_check
     >> exchange_data_from_staging
     >> drop_staging_table
     )


aggregate_kafka_events_dag()
