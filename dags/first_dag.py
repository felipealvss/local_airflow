from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

with DAG(
    'my_first_dag',
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:
    task1 = EmptyOperator(task_id='task1')
    task2 = EmptyOperator(task_id='task2')
    task3 = EmptyOperator(task_id='task3')
    task4 = BashOperator(
        task_id='task4_mkdir',
        bash_command='mkdir -p "/opt/airflow/directory"'
    )
    task1 >> [task2, task3]
    task3 >> task4
