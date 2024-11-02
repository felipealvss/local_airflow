from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add
import pendulum
import pandas as pd

# Definicao de constants

# Definicao de functions
def extrai_dados(data_interval_end):

    # Parametros da API
    city = 'Fortaleza,Ceara'
    key = 'DU4XK9HB6UWSY8HMYF5NUM83W'
    unitGroup = 'metric'
    include = 'days'
    contentType = 'csv'

    url = f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup={unitGroup}&include={include}&key={key}&contentType={contentType}'

    dados = pd.read_csv(url)

    file_path = f'/opt/airflow/week={data_interval_end}/'

    dados.to_csv(file_path + 'dados_brutos.csv')
    dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
    dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')

# Definicao de DAG
with DAG(
    'api_visualcrossing_consumer',
    start_date=pendulum.datetime(2024, 9, 30, tz='UTC'),
    schedule_interval='0 0 * * 1' # executar toda segunda-feira
) as dag:
    task01 = BashOperator(
        task_id='task01_mkdir',
        bash_command='mkdir -p "/opt/airflow/week={{ data_interval_end.strftime("%Y-%m-%d") }}"'
    )
    task02 = PythonOperator(
        task_id='task02_obtain_data',
        python_callable=extrai_dados,
        op_kwargs={
            'data_interval_end': '{{ data_interval_end.strftime("%Y-%m-%d") }}'
        }
    )
    # Fluxo DAG
    task01 >> task02