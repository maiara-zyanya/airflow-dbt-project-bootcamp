import trino
# The way we're doing the DQ check here is
def run_trino_query_dq_check(query):
    results = execute_trino_query(query)
    if len(results) == 0:
        raise ValueError('The query returned no results!')
    for result in results:
        for column in result:
            if type(column) is bool:
                assert column is True


def execute_trino_query(query):
    conn = trino.dbapi.connect(
        host='dataengineer-eczachly.trino.galaxy.starburst.io',
        port=443,
        user='support@eczachly.com/student',
        http_scheme='https',
        catalog='academy',
        auth=trino.auth.BasicAuthentication('support@eczachly.com/student', 'trin0-supp0rt!'),
    )
    print(query)
    cursor = conn.cursor()
    print("Executing query for the first time...")
    cursor.execute(query)
    return cursor.fetchall()
