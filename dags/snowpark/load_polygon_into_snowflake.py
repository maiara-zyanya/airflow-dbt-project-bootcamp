import os
import requests
import ast
from include.eczachly.snowflake_queries import get_snowpark_session


def load_snowflake(polygon_key=None):
    session = get_snowpark_session()
    print('api key is', polygon_key)
    polygon_key = os.environ.get('POLYGON_API_KEY') or ast.literal_eval(polygon_key)['AWS_SECRET_ACCESS_KEY']
    url = 'https://api.polygon.io/v3/reference/tickers?active=true&limit=1000&apiKey=' + polygon_key
    example_ticker = {'ticker': 'GT',
                      'name': 'Goodyear Tire & Rubber',
                      'market': 'stocks',
                      'locale': 'us',
                      'primary_exchange': 'XNAS',
                      'type': 'CS',
                      'active': True,
                      'currency_name': 'usd',
                      'cik': '0000042582',
                      'composite_figi': 'BBG000BKNX95',
                      'share_class_figi': 'BBG001S5RQ62',
                      'last_updated_utc': '2024-10-30T00:00:00Z'
    }
    response = requests.get(url).json()

    tickers = response['results']
    table = 'bootcamp.stock_tickers'

    while 'next_url' in response:
        print('calling again')
        url = response['next_url'] + '&apiKey=' + polygon_key
        print(url)
        response = requests.get(url).json()
        tickers.extend(response['results'])

    print('we found', len(tickers), 'tickers')
    columns = []
    for (key, value) in example_ticker.items():
        if 'utc' in key:
            columns.append(key + ' TIMESTAMP')
        elif type(value) is str:
            columns.append(key + ' VARCHAR')
        elif type(value) is bool:
            columns.append(key + ' BOOLEAN')
        elif type(value) is int:
            columns.append(key + ' INTEGER')

    columns_str = ' , '.join(columns)
    create_ddl = f'CREATE TABLE IF NOT EXISTS {table} ({columns_str})'
    print(create_ddl)
    session.sql(create_ddl)

    dataframe = session.create_dataframe(tickers, schema=example_ticker.keys())
    dataframe.write.mode("overwrite").save_as_table(table)










