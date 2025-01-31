from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import datetime, timedelta
from include.eczachly.trino_queries import execute_trino_query, run_trino_query_dq_check
from airflow.models import Variable
from include.eczachly.aws_secret_manager import get_secret
from datetime import datetime, timezone
import ast
import requests
#import os
import pandas_market_calendars as mcal

tabular_credential = Variable.get("TABULAR_CREDENTIAL")
polygon_credentials = Variable.get('POLYGON_CREDENTIALS', deserialize_json=True)['AWS_SECRET_ACCESS_KEY']
polygon_api_key = ast.literal_eval(get_secret("POLYGON_CREDENTIALS"))['AWS_SECRET_ACCESS_KEY']
# TODO make sure to rename this if you're testing this dag out!
schema = 'prachisharma4326'
@dag(
    description="A dag for your homework, it takes polygon data in and cumulates it",
    default_args={
        "owner": schema,
        "start_date": datetime(2025, 1, 10),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 1, 10),
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=True,
    tags=["community"],
)
def starter_dag():
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']
    production_table = f'{schema}.maang_stock_prices'
    staging_table = production_table + '_stg_{{ ds_nodash }}'
    cumulative_table = f'{schema}.maang_stock_prices_cummulated'
    yesterday_ds = '{{ yesterday_ds }}'
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

    # todo figure out how to load data from polygon into Iceberg
    def load_data_from_polygon(table, ds, **kwargs):
        """Load stock data from Polygon API into Iceberg staging table."""
        for ticker in maang_stocks:
            polygon_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{ds}/{ds}?adjusted=true&sort=asc&apiKey={polygon_api_key}"
            
            response = requests.get(polygon_url)
            if response.status_code == 200:
                results = response.json().get("results", [])
            else:
                raise ValueError(f"Failed to fetch data for ticker {ticker}: {response.status_code}")

            rows = []
            for result in results:
                open_ = result.get("o", 0.0)
                high_ = result.get("h", 0.0)
                low_ = result.get("l", 0.0)
                close_ = result.get("c", 0.0)
                volume_ = result.get("v", 0.0)
                row_str = f"('{ticker}', DATE '{ds}', {open_}, {high_}, {low_}, {close_}, {volume_})"
                #ensure no duplicates
                if row_str not in rows:
                    rows.append(row_str)

            if rows:
                values_str = ",\n".join(rows)
                query = f"""
                INSERT INTO {table} (ticker, date, open, high, low, close, volume)
                VALUES {values_str}
                """
                execute_trino_query(query=query)

    # TODO create schema for daily stock price summary table
    create_daily_step = PythonOperator(
        task_id="create_daily_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {production_table} (
                            ticker VARCHAR,
                            date DATE,
                            open DOUBLE,
                            high DOUBLE,
                            low DOUBLE,
                            close DOUBLE,
                            volume BIGINT
                        )
                        WITH (
                        format = 'PARQUET',
                         partitioning = ARRAY['date']
                        )
                    """
        }
    )

    create_staging_step = PythonOperator(
        task_id="create_staging_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {staging_table} (
                            ticker VARCHAR,
                            date DATE,
                            open DOUBLE,
                            high DOUBLE,
                            low DOUBLE,
                            close DOUBLE,
                            volume BIGINT
                        )
                        WITH (
                        format = 'PARQUET',
                         partitioning = ARRAY['date']
                        )
                    """
        }
    )

    clear_staging_step = PythonOperator(
        task_id="clear_staging_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                DELETE FROM {staging_table} 
                WHERE date = DATE('{ds}')
                """
        }
    )

    # todo make sure you load into the staging table not production
    load_to_staging_step = PythonOperator(
        task_id="load_to_staging_step",
        python_callable=load_data_from_polygon,
        op_kwargs={
            'table': staging_table
        },
        provide_context=True  # Ensure context is available
    )

    # TODO figure out some nice data quality checks
    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={
            'query': f"""
                SELECT COUNT(*) AS total_rows, 
                        COUNT(DISTINCT ticker) AS unique_symbols 
                    FROM {staging_table}
                    WHERE date = DATE('{ds}')
            """
        }
    )

    # todo make sure you clear out production to make things idempotent
    clear_step = PythonOperator(
        task_id="clear_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""DELETE FROM {production_table} WHERE date = DATE('{ds}')"""
        }
    )

    exchange_data_from_staging = PythonOperator(
        task_id="exchange_data_from_staging",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                INSERT INTO {production_table}
                SELECT DISTINCT * FROM {staging_table}
                WHERE date = DATE('{ds}')
            """
        }
    )

    # TODO do not forget to clean up
    drop_staging_table = PythonOperator(
        task_id="drop_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"DROP TABLE IF EXISTS {staging_table}"
        }
    )

    # TODO create the schema for your cumulated table
    create_cumulation_step = PythonOperator(
        task_id="create_cumulation_step",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {cumulative_table} (
                        ticker VARCHAR,
                        prices ARRAY<DOUBLE>,
                        volumes ARRAY<BIGINT>,
                        date DATE
                    )
                    WITH (
                        format = 'PARQUET',
                         partitioning = ARRAY['date']
                        )
                """
        }
    )

    # TODO make sure you create array metrics for the last 7 days of stock prices
    cumulate_step = PythonOperator(
        task_id="cumulate_step",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': 
            f"""
            INSERT INTO {cumulative_table}
            WITH most_recent_ticker_array AS (
                SELECT 
                    ticker, 
                    prices, 
                    volumes,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS row_num
                FROM prachisharma4326.maang_stock_prices_cummulated
            ),
            last_ticker_prices AS (
                SELECT 
                    ticker, 
                    CAST(
                        ARRAY_AGG(prices[1]) 
                        AS ARRAY(double)
                    ) AS prices,    
                    CAST(
                        ARRAY_AGG(CAST(volumes[1] AS BIGINT)) 
                        AS ARRAY(bigint)
                    ) AS volumes 
                FROM most_recent_ticker_array 
                WHERE row_num = 1 
                GROUP BY ticker
            ),
            today_array AS (
                SELECT 
                    ticker,
                    CAST(ARRAY [low, high, open, close] AS ARRAY(double)) AS prices_today, -- Flatten array of rows
                    CAST(ARRAY [CAST(volume AS BIGINT)] AS ARRAY(bigint)) AS volumes_today -- Flatten array of rows
                FROM prachisharma4326.maang_stock_prices
                WHERE date = DATE('{ds}')
            ),
            combined AS (
                SELECT 
                    COALESCE(lt.ticker, ta.ticker) AS ticker,
                    CASE 
                        WHEN lt.ticker IS NULL THEN ta.prices_today
                        WHEN ta.ticker IS NULL THEN lt.prices
                        ELSE CONCAT(ta.prices_today, lt.prices) -- Concatenate arrays of double
                    END AS prices,
                    CASE 
                        WHEN lt.ticker IS NULL THEN ta.volumes_today
                        WHEN ta.ticker IS NULL THEN lt.volumes
                        ELSE CONCAT(ta.volumes_today, lt.volumes) -- Concatenate arrays of bigint
                    END AS volumes
                FROM last_ticker_prices AS lt 
                FULL OUTER JOIN today_array AS ta
                ON lt.ticker = ta.ticker
            )
            SELECT 
                ticker,
                prices,
                volumes,
                DATE('{ds}') AS date
            FROM combined
            """
        }
    )

    # TODO figure out the right dependency chain
    (check_market_day 
     >> create_daily_step 
     >> create_staging_step 
     >> clear_staging_step 
     >> load_to_staging_step 
     >> run_dq_check 
     >> clear_step 
     >> exchange_data_from_staging 
     >> drop_staging_table
     >> create_cumulation_step
     >> cumulate_step)


starter_dag()
