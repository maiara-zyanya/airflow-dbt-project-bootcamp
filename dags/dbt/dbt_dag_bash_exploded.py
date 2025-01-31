import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

dbt_env_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dbt_project', 'dbt.env')
load_dotenv(dbt_env_path)

airflow_home=os.environ['AIRFLOW_HOME']

# Define paths to the DBT project and virtual environment
PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_project'
PATH_TO_DBT_VENV = f'{airflow_home}/dbt_venv/bin/activate'

default_args = {
  "owner": "Bruno de Lima",
  "retries": 0,
  "execution_timeout": timedelta(hours=1),
}

@dag(
    start_date=datetime(2024, 5, 9),
    schedule='@once',
    catchup=False,
    default_args=default_args,
)
def dbt_dag_bash_exploded():
    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    dbt_stg_orders = BashOperator(
        task_id='dbt_stg_orders',
        bash_command=f'dbt build -s stg_customers',
        cwd=PATH_TO_DBT_PROJECT,
    )

    dbt_stg_payments = BashOperator(
        task_id='dbt_stg_payments',
        bash_command=f'dbt build -s stg_customers',
        cwd=PATH_TO_DBT_PROJECT,
    )

    dbt_fact_orders = BashOperator(
        task_id='dbt_fact_orders',
        bash_command=f'dbt build -s fact_orders',
        cwd=PATH_TO_DBT_PROJECT,
    )

    # Post DBT workflow task
    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow", trigger_rule="all_done")

    # Define task dependencies
    pre_dbt_workflow >> [dbt_stg_orders, dbt_stg_payments] >> dbt_fact_orders >> post_dbt_workflow


dbt_dag_bash_exploded()