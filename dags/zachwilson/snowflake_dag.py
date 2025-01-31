from airflow.decorators import dag, task
from datetime import datetime
from airflow.models import Variable
from dags.snowpark.load_polygon_into_snowflake import load_snowflake

# Define DAG using @dag decorator
@dag(
    # The name of your DAG and the name of the Python file should match.
    # Both should start with your GitHub username to avoid duplicate DAG names.
    "snowflake_dag",
    description="Zach Wilson's Capstone DAG",
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
def snowflake_dag():
    @task(task_id='load_polygon_into_snowflake')
    def load_snowflake_task(polygon_key):
        load_snowflake(polygon_key)

    credentials = Variable.get('POLYGON_CREDENTIALS')
    load_snowflake_example = load_snowflake_task(credentials)

snowflake_dag()
