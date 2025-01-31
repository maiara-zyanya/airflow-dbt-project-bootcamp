import os
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta
import time
TOKEN = {'access_token': None}
EXPIRATION_DATE = datetime.min

def get_tabular_access_token(tabular_credential):
    TOKEN_URL = 'https://api.tabular.io/ws/v1/oauth/tokens'
    client_id, client_secret = tabular_credential.split(':')
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    body = urlencode({
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    })

    response = requests.post(TOKEN_URL, headers=headers, data=body)
    return response.json()


def query_table_partitions(table_name: str,
                           partition_path: str,
                           tabular_credential: str,
                           warehouse: str = 'ce557692-2f28-41e8-8250-8608042d2acb'
                        ) -> bool:
    global TOKEN, EXPIRATION_DATE

    # Check if the token is expired or missing, then refresh it
    if not TOKEN['access_token'] or EXPIRATION_DATE < datetime.now():
        TOKEN = get_tabular_access_token(tabular_credential)
        EXPIRATION_DATE = datetime.now() + timedelta(days=1)

    database = table_name.split('.')[0]
    table = table_name.split('.')[1]
    access_token = TOKEN['access_token']
    url = f'https://api.tabular.io/ws/v1/ice/warehouses/{warehouse}/namespaces/{database}/tables/{table}'

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Bearer {access_token}',
    }

    response = requests.get(url, headers=headers)
    partitions = response.json()
    full_partition_path = f'partitions.{partition_path}'
    for snapshot in partitions['metadata']['snapshots']:
        summary = snapshot['summary']
        if full_partition_path in summary:
            return True

    return False


def poke_tabular_partition(table, partition, tabular_credential):
    found_partition = query_table_partitions(table, partition, tabular_credential)
    while not found_partition:
        # wait 30 seconds before trying again
        print(f'Partition {partition} for table {table} not found! Trying again in a minute!')
        time.sleep(60)
        found_partition = query_table_partitions(table, partition, tabular_credential)


# poke_tabular_partition('bootcamp.nba_player_seasons', 'season=2023')


