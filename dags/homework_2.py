import requests as req  # для выполнения HTTP-запросов
import pandas as pd  # для обработки данных
from datetime import datetime, timedelta  # для работы с датами
import json  # для парсинга json
from clickhouse_driver import Client  # для подключения к ClickHouse
from airflow import DAG
from airflow.operators.python import PythonOperator

URL = 'https://api.exchangerate.host/timeframe?'
params = {'access_key': '422bab0c9e08a5476912061877017b8d', 'source': 'USD', 'start_date': '2023-01-01',
          'end_date': '2023-01-01'}
my_folder = '/usr/local/airflow/dags/sandbox/646053956/'
# Настройка подключения к базе данных ClickHouse
CH_CLIENT = Client(
    host='158.160.116.58',  # IP-адрес сервера ClickHouse
    user='student',  # Имя пользователя для подключения
    password='sdf4wgw3r',  # Пароль для подключения
    database='sandbox'  # База данных, к которой подключаемся
)


# Функция для извлечения данных с API курса валют и сохранения их в локальный файл
def extract_data(**kwargs):
    """
    Эта функция выгружает данные по валютам, используя GET-запрос,
    и сохраняет результат в локальный файл `s_file`.
    """
    resp = req.get(url=kwargs['url'], params=kwargs['params'])
    with open(kwargs['s_file'], 'w', encoding='utf-8') as f:
        json.dump(resp.json(), f)


# Функция для обработки данных в формате json и преобразования их в CSV
def transform_data(**kwargs):
    """
    Эта функция обрабатывает полученные данные в формате JSON
    и преобразует их в табличном формате для дальнейшей работы.
    В конце данные записываются в CSV файл
    """
    rows = []
    with open(kwargs['s_file'], 'r', encoding='utf-8') as f:
        for k, v in json.load(f).get('quotes').get("2023-01-01").items():
            rows.append(('2023-01-01', 'USD', k[3:], v))

    df = pd.DataFrame(rows, columns=['date', 'currency_source', 'currency', 'value'])
    df.to_csv(kwargs['csv_file'], encoding='utf-8', index=False)


# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(**kwargs):
    """
    Эта функция считывает CSV файл, создает таблицу в
    базе данных ClickHouse и добавляет данные в неё
    """
    # Чтение данных из CSV
    data_frame = pd.read_csv(kwargs['csv_file'])

    # Создание таблицы, ЕСЛИ НЕ СУЩЕСТВУЕТ ТО СОЗДАТЬ ТАБЛИЦУ
    kwargs['client'].execute(
        'CREATE TABLE IF NOT EXISTS exchange_rates_usd_new (`date` String, currency_source String, currency String, value float) ENGINE Log')

    # Запись data frame в ClickHouse
    kwargs['client'].execute('INSERT INTO exchange_rates_usd_new VALUES', data_frame.to_dict('records'))


default_args = {
    'start_date': datetime(2024, 1, 1),
    'schedule_interval': None,
}

dag = DAG(dag_id='646053956', default_args=default_args, tags=['646053956'])

extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data,
                              op_kwargs={'url': URL, 'params': params, 's_file': f'{my_folder}currency.json'}, dag=dag)

transform_task = PythonOperator(
    task_id='transform_task', python_callable=transform_data,
    op_kwargs={'s_file': f'{my_folder}currency.json', 'csv_file': f'{my_folder}currency.csv'}, dag=dag
)

upload_task = PythonOperator(
    task_id='upload_task', python_callable=upload_to_clickhouse,
    op_kwargs={'csv_file': f'{my_folder}currency.csv', 'client': CH_CLIENT}, dag=dag
)

extract_data >> transform_task >> upload_task
