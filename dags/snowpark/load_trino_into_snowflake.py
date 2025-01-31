from include.eczachly.trino_queries import run_trino_query_dq_check, execute_trino_query
from include.eczachly.snowflake_queries import get_snowpark_session


def get_data_and_schema_from_trino(table):
    # Define Snowflake connection parameters
    # Create a Snowpark session
    snowflake_session = get_snowpark_session('bootcamp')
    data = execute_trino_query(f'SELECT * FROM {table}')
    print('We found {} rows'.format(len(data)))
    schema = execute_trino_query(f'DESCRIBE {table}')
    columns = []
    column_names = []
    for column in schema:
        column_names.append(column[0])
        columns.append(' '.join(column))
    current_config = snowflake_session.sql("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()
    print(f"Using warehouse: {current_config[0][0]}, database: {current_config[0][1]}, schema: {current_config[0][2]}")
    columns_str = ','.join(columns)
    create_ddl = f'CREATE TABLE IF NOT EXISTS {table} ({columns_str})'
    print(create_ddl)
    snowflake_session.sql(create_ddl)
    write_df = snowflake_session.create_dataframe(data, schema=column_names)
    write_df.write.mode("overwrite").save_as_table(table)
    snowflake_session.close()


get_data_and_schema_from_trino('bootcamp.web_events')

