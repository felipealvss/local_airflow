import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta
import requests as req

# Intervalo de datas
data_inicio = datetime.today()
data_fim = data_inicio + timedelta(days=7)

# Formatando as datas
data_inicio = data_inicio.strftime("%Y-%m-%d")
data_fim = data_fim.strftime("%Y-%m-%d")

city = 'Fortaleza,Ceara'
key = 'DU4XK9HB6UWSY8HMYF5NUM83W'
unitGroup = 'metric'
include = 'days'
contentType = 'csv'

url = f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{data_inicio}/{data_fim}?unitGroup={unitGroup}&include={include}&key={key}&contentType={contentType}'

dados = pd.read_csv(url)
print(dados.head())

file_path = f'D:/TESTES_PY/local_airflow/semana={data_inicio}/'
os.mkdir(file_path)

dados.to_csv(file_path + 'dados_brutos.csv')
dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')
