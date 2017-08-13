from datetime import datetime, timedelta
import json

from airflow.hooks import HttpHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import PythonOperator,
from airflow.models import DAG

PAIR = 'eth/usd'

def get_rates(ds, **kwargs):
    # connection: a Airflow connection
    pg_hook = PostgresHook(postgres_conn_id='crypto')
    api_hook = HttpHook(http_conn_id='cryptocoincharts_eth', method='GET')

    resp = api_hook.run('')
    resp = json.loads(resp.content)

    rates_insert = """INSERT INTO rates (id, price, last_price, volume)
                      VALUES (%s, %s, %s, %s);"""
    markets_insert = """INSERT INTO markets (market, pair, price, volume_btc, volume)
                      VALUES (%s, %s, %s, %s, %s);"""

    pg_hook.run(rates_insert, parameters=(resp['id'], resp['price'], resp['price_before_24h'], resp['volume_second']))

    for market in resp['markets']:
        pg_hook.run(markets_insert, parameters=(market['market'], PAIR, market['price'], market['volume_btc'], market['volume']))


args = {
    'owner': 'mikeghen',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG(dag_id='eth_rates',
          default_args=args,
          schedule_interval='0 */5 * * *',
          dagrun_timeout=timedelta(seconds=5))

get_rates_task = \
    PythonOperator(task_id='get_rates',
                   provide_context=True,
                   python_callable=get_rates,
                   dag=dag)
