from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from spotify import get_top_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG('spotify_top_data_pipeline', default_args=default_args, schedule_interval='@daily')

get_top_data_task = PythonOperator(
    task_id='get_top_data',
    python_callable=get_top_data,
    dag=dag
)

get_top_data_task
