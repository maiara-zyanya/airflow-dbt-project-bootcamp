import os
from datetime import datetime, timedelta
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag
from dotenv import load_dotenv

dbt_env_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dbt_project', 'dbt.env')
load_dotenv(dbt_env_path)

# Retrieve environment variables
airflow_home = os.getenv('AIRFLOW_HOME')
PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_project'
PATH_TO_DBT_PROFILES = f'{airflow_home}/dbt_project/profiles.yml'

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)

default_args = {
  "owner": "Bruno de Lima",
  "retries": 0,
  "execution_timeout": timedelta(hours=1),
}

dbt_dag_cosmos_DbtDag = DbtDag(
    dag_id="dbt_dag_cosmos_task_group_DbtDag",
    project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
    profile_config=profile_config,
    render_config=RenderConfig(
        select=["+fact_orders"],
    ),
    start_date=datetime(2024, 5, 9),
    schedule='@once',
    catchup=False,
    default_args=default_args,
)
