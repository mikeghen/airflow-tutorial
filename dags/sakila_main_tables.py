from airflow import DAG
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from datetime import datetime, timedelta

"""
Extracts most tables from sakila_1 and sakila_2 to BigQuery

Prerequisties:
1. sakila_1, sakila_2 connection in Airflow (MySQL)
2. gcp_test connection in Airflow (Google Cloud Platform)
3. ghen-airflow bucket in Google Cloud Storage
  a. Make path to ghen-airflow/sakila_1 and ghen-airflow/sakila_2
4. sakila_1, sakila_2 datasets in BigQuerys
"""

sakila_connections = [
# NOTE: Also the names of the databases in MySQL and datasets in BigQuery
    'sakila_1',
    'sakila_2'
]

sakila_tables = [
    'actor',
    # 'address', NOTE: This table has some sketchy encoding, use another DAG to load
    'category',
    'city',
    'country',
    'customer',
    'film',
    'film_actor',
    'film_category',
    'film_text',
    'inventory',
    'language',
    'payment',
    'rental',
    # 'staff', NOTE: This table has some sketchy encoding, use another DAG to load
    'store'
]

default_args = {
    'owner': 'mikeghen',
    'start_date': datetime(2017, 8, 11),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('sakila_main_tables', default_args=default_args, schedule_interval=timedelta(days=1))

slack_notify = SlackAPIPostOperator(
    task_id='slack_notify',
    token='xxxxxx',
    channel='data-status',
    username='airflow',
    text='Successfully performed sakila ETL operation',
    dag=dag)

for connection in sakila_connections:
    for table in sakila_tables:
        extract = MySqlToGoogleCloudStorageOperator(
            task_id="extract_mysql_%s_%s"%(connection,table),
            mysql_conn_id=connection,
            google_cloud_storage_conn_id='gcp_test',
            sql="SELECT *, '%s' as source FROM sakila.%s"%(connection,table),
            bucket='ghen-airflow',
            filename="%s/%s/%s{}.json"%(connection,table,table),
            schema_filename="%s/schemas/%s.json"%(connection,table),
            dag=dag)

        load = GoogleCloudStorageToBigQueryOperator(
            task_id="load_bq_%s_%s"%(connection,table),
            bigquery_conn_id='gcp_test',
            google_cloud_storage_conn_id='gcp_test',
            bucket='ghen-airflow',
            destination_project_dataset_table="spark-test-173322.%s.%s"%(connection,table),
            source_objects=["%s/%s/%s*.json"%(connection,table,table)],
            schema_object="%s/schemas/%s.json"%(connection,table),
            source_format='NEWLINE_DELIMITED_JSON',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE',
            project_id='spark-test-173322',
            dag=dag)

        load.set_upstream(extract)
        slack_notify.set_upstream(load)
