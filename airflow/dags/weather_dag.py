import json
from datetime import datetime
from time import sleep

import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import requests
import sqlalchemy.exc
from airflow.models import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from psql_cli import PsqlClient
import time


BASE_URL = 'https://history.openweathermap.org/data/2.5/history/city'
API_KEY = '44512c3fcb195eeaea99742342f8d292'

SQL_TABLE = 'weather_daily'
SQL_CREATE = f"""
CREATE TABLE IF NOT EXISTS {SQL_TABLE} (
dt integer,
province TEXT,
temp_max decimal(16,2),
temp_min decimal(16,2),
weather_main TEXT,
weather_desc TEXT,
UNIQUE(dt,province)
)
"""
SQL_REPORT = f"""
SELECT symbol, avg_num_trades
FROM {SQL_TABLE}
WHERE date = '{{date}}'
ORDER BY avg_num_trades DESC
LIMIT 1
"""

PROVINCES = {'BUENOS_AIRES': {
    'lat':-34.61315, 
    'lon':-58.37723
},
'CORDOBA': {
    'lat':-31.4135, 
    'lon':-64.18105
},
'SANTA_FE': {
    'lat': -32.94682, 
    'lon': -60.63932
},
'JUJUY': {
    'lat': -24.19457, 
    'lon': -65.29712
},
'TIERRA_DEL_FUEGO': {
    'lat': -54.8064, 
    'lon': -68.305 
}
}

def kelvin_to_celsius(kevin):
    return float("{:.2f}".format(float(kevin) - 273,15))


def _get_weather_data(province, lat, lon, **context):
    now = int( time.time() )
    start = 1610788777 #hardcoded
    end_point = (
        f"{BASE_URL}?&lat={lat}&lon={lon}&appid={API_KEY}&cnt=168"
    )
    print(f"Getting data from {end_point}...")
    r = requests.get(end_point)
    sleep(15)  # To avoid api limits
    data = json.loads(r.content)
    df = (pd.DataFrame.from_dict(data['list'], orient='columns'))
    df['province'] = province
    df['weather_main'] = df['weather'].apply(lambda x: x[0]['main'])
    df['weather_desc'] = df['weather'].apply(lambda x: x[0]['description'])
    df['temp_max'] = df['main'].apply(lambda x: kelvin_to_celsius(x['temp_max']))
    df['temp_min'] = df['main'].apply(lambda x: kelvin_to_celsius(x['temp_min']))
    df = df[['dt', 'province', 'temp_max', 'temp_min','weather_main', 'weather_desc']]
    return df


def _insert_daily_data(**context):
    task_instance = context['ti']
    # Get xcom for each upstream task
    dfs = []
    for province in PROVINCES:
        dfs.append(task_instance.xcom_pull(task_ids=f'get_daily_data_{province}'))
    df = pd.concat(dfs, axis=0)
    conn = BaseHook.get_connection('postgres_weather')
    db_uri = f"postgres://{conn.login}:{conn.password}@{conn.host}/{conn.schema}"
    print(db_uri)
    sql_cli = PsqlClient(db_uri)
    try:
        sql_cli.insert_from_frame(df, SQL_TABLE)
        print(f"Inserted {len(df)} records")
    except sqlalchemy.exc.IntegrityError:
        # You can avoid doing this by setting a trigger rule in the reports operator
        print("Data already exists! Nothing to do...")
    return



default_args = {
    'owner': 'nmarcantonio',
    'retries': 0,
    'start_date': datetime(2021, 11, 22),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['nmarcantonio@itba.edu.ar'],
}
with DAG(
    'weather', default_args=default_args, schedule_interval='0 4 * * *'
) as dag:

    create_table_if_not_exists = PostgresOperator(
        task_id='create_table_if_not_exists',
        sql=SQL_CREATE,
        postgres_conn_id='postgres_weather',
    )

    # Create several task in loop
    get_data_task = {}
    for province, info in PROVINCES.items():
        get_data_task[province] = PythonOperator(
            task_id=f'get_daily_data_{province}',
            python_callable=_get_weather_data,
            op_args=[province, info['lat'], info['lon']],
            provide_context=True,
        )

    insert_daily_data = PythonOperator(
        task_id='insert_daily_data',
        python_callable=_insert_daily_data,
        provide_context=True,
    )

    for province in PROVINCES:
        upstream_task = create_table_if_not_exists
        task = get_data_task[province]
        upstream_task.set_downstream(task)
        task.set_downstream(insert_daily_data)
