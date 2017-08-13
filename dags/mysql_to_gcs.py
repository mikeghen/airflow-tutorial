from airflow import DAG
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mikeghen',
    'start_date': datetime(2017, 8, 11),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('mysql_to_gcs', default_args=default_args)

export_actor = MySqlToGoogleCloudStorageOperator(
    task_id='extract_actors',
    mysql_conn_id='sakila_test',
    google_cloud_storage_conn_id='gcp_test',
    sql='SELECT * FROM sakila.actor',
    bucket='ghen-airflow',
    filename='sakila/actors/actors{}.json',
    schema_filename='sakila/schemas/actors.json',
    dag=dag)
