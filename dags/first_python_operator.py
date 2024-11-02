from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

def cumprimentos():
    print("boas vindas ao Airflow!")

with DAG(
    'my_python_operator',
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='task1_python_msg',
        python_callable=cumprimentos        
    )