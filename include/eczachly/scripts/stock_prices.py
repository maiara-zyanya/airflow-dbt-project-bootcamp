import pyarrow as pa
#from aws_secret_manager import get_secret
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform, DayTransform
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DateType, DoubleType

from dotenv import load_dotenv
from ast import literal_eval
from datetime import date, timedelta, datetime
import requests
import pandas as pd
load_dotenv()

def create_daily_partitioned_table(catalog, schema_name, table_name):
    # Define Iceberg-compatible schema
    iceberg_schema = Schema(
        NestedField(1, "ticker", StringType(), required=True),
        NestedField(2, "date", DateType(), required=True),
        NestedField(3, "open", DoubleType(), required=True),
        NestedField(4, "high", DoubleType(), required=True),
        NestedField(5, "low", DoubleType(), required=True),
        NestedField(6, "close", DoubleType(), required=True),
        NestedField(7, "volume", DoubleType(), required=True),
    )
    # Define partition spec based on Iceberg schema
    # Create partition on the day it is loaded
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=2, field_id=1000, transform=DayTransform(), name="datetime_day"
        )
    )
    table_identifier = f"{schema_name}.{table_name}"
    # Create the table
    catalog.create_table_if_not_exists(
        identifier=table_identifier,
        schema=iceberg_schema,
        partition_spec=partition_spec
        )
    return catalog.load_table(table_identifier)

def parse_api_response(api_response):
    # Parse the JSON response and transform it into rows for Iceberg
    rows = []
    for result in api_response["results"]:
        date = datetime.utcfromtimestamp(result["t"] // 1000).date()
        row = {
            "ticker": api_response["ticker"],
            "date": date,
            "open": result["o"],
            "high": result["h"],
            "low": result["l"],
            "close": result["c"],
            "volume": result["v"]
        }
        rows.append(row)
    return rows


def homework_script(table_name,schema_name):
    # Stocks to fetch
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']

    # polygon api credential
    polygon_api_key = literal_eval(get_secret("POLYGON_CREDENTIALS"))['AWS_SECRET_ACCESS_KEY']
    # Use today's date
    today = date.today()
    yesterday = today - timedelta(days=1)
    catalog = load_catalog(
        'academy',
        type="rest",  # Explicitly define the catalog type as REST
        uri="https://api.tabular.io/ws",  # Your REST catalog endpoint
        warehouse=get_secret("CATALOG_NAME"),
        credential=get_secret('TABULAR_CREDENTIAL')
    )
    
    ## Todo create your partitioned Iceberg table
    #table_name = "maang_stock_prices"
    #schema_name = "prachisharma4326"
    table = create_daily_partitioned_table(catalog, schema_name, table_name)

    ## TODO read data from the Polygon API and load it into the Iceberg table branch
    for ticker in maang_stocks:
        print (f'Fetching {ticker}')
        polygon_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{yesterday}/{today}?adjusted=true&sort=asc&apiKey={polygon_api_key}"
        #print(polygon_url)
        if api_response.status_code != 200:
            print(f"Error fetching data for {ticker}: {api_response.status_code}")
            continue
        api_response = requests.get(polygon_url)
        api_data = api_response.json()
        rows = parse_api_response(api_data)
        print(rows)
        
        # Convert rows to DataFrame and then to PyArrow Table
        # Define the schema with required fields
        schema = pa.schema([
            pa.field('ticker', pa.string(), nullable=False),
            pa.field('date', pa.date32(), nullable=False),
            pa.field('open', pa.float64(), nullable=False),
            pa.field('high', pa.float64(), nullable=False),
            pa.field('low', pa.float64(), nullable=False),
            pa.field('close', pa.float64(), nullable=False),
            pa.field('volume', pa.float64(), nullable=False)
        ])
        df = pd.DataFrame(rows)  # Convert list of rows to DataFrame
        pa_table = pa.Table.from_pandas(df, schema=schema)  # Convert DataFrame to PyArrow Table
        table.append(pa_table)  # Append to Iceberg table


    ## TODO create a branch table.create_branch()
    # Create a branch
    if not list(table.snapshots()):
        empty_data = pa.Table.from_pylist([])  # Create an empty PyArrow table
        table.append(empty_data)
    current_snapshot_id = table.current_snapshot().snapshot_id

    print(f'...Creating branch on current snapshot {current_snapshot_id}...')
    table.manage_snapshots().create_branch(current_snapshot_id,"audit_branch").commit()
    print("A new branch {audit_branch} committed.")

##Removed this code to allow direct execution
#if __name__ == "__main__":
 #   homework_script(schema_name, table_name)