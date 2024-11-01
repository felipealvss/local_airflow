from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 24),
}

dag = DAG(
    'hello_world',
    default_args=default_args,
    schedule_interval='@daily',
)

start = EmptyOperator(task_id='start', dag=dag)

logger.info("DAG hello_world carregado com sucesso!")