from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine, text
import pandas as pd
from airflow.utils.dates import days_ago

import requests
from airflow.exceptions import AirflowException
from sqlalchemy import Table, Column, Integer, String, MetaData, VARCHAR

default_args = {
    'owner': '@max',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'schedule_interval': None,
}

conn = BaseHook.get_connection('psql_connection')
HOST = conn.host
USER = conn.login
SCHEMA = conn.schema
PASSWORD = conn.password
PORT = conn.port

PS_CLIENT = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{SCHEMA}')

# Создадим объект класса DAG
dag = DAG('test_xcom_saf', schedule_interval='@daily', start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 4),
          tags=['examples_saf'])


def fetch_data_to_xcom(api_url, **kwargs):
    # Получаем все файлы из Xcom на вчера, если такого ключа нет, то будет возвращен None
    task_instance = kwargs['task_instance']
    files = task_instance.xcom_pull(key='files_full',
                                    include_prior_dates=True)  # Данный параметр включает доступ к Xcom для всех предыдущих запусков, из одинаковых ключей будет выбран последний ключ по времени
    if files is None:
        files = []
    # Получаем данные с сервера
    response = requests.get(api_url + kwargs['ds'])

    if response.status_code == 200:
        # Парсинг JSON ответа
        data = response.json()["files"]
        # Находим разницу в 2 массивах
        task_instance.xcom_push(key='file_difference', value=list(set(data) ^ set(files)))
        # Отправляем разницу в Xcom с другим ключем
        task_instance.xcom_push(key='files_full', value=list(set(files) | set(data)))

    else:
        raise AirflowException(f"Request failed {response.status_code}")


# Функция для загрузки данных в ClickHouse из CSV
def upload_to_postgres(url, table_name, engine, **kwargs):
    metabata = MetaData()
    campaign_table = Table('campaign_table', metabata,
                           Column('campaign', String),
                           Column('cost', Integer),
                           Column('date', String))

    # Получаем разницу в файлах сегодня и вчера
    task_instance = kwargs['task_instance']
    files = task_instance.xcom_pull(key='file_difference')
    # Создание таблицы, ЕСЛИ НЕ СУЩЕСТВУЕТ ТО СОЗДАТЬ ТАБЛИЦУ
    metabata.drop_all(engine)
    metabata.create_all(engine)

    # Итеративно проходимся по файлам и добавляем в ClickHouse
    for file in files:
        # Чтение данных из CSV
        data_frame = pd.read_csv(url + file)

        # Запись data frame в Postgres
        data_frame.to_sql(
            name=workers_table,  # имя таблицы
            con=engine,  # движок
            if_exists="append",  # если таблица уже существует, добавляем
            index=False  # без индекса
        )


fetch_data_to_xcom = PythonOperator(
    task_id='fetch_data_to_xcom',
    python_callable=fetch_data_to_xcom,
    op_args=['http://158.160.116.58:4009/files/'],
    dag=dag,
)

# Задачи для загрузки данных
upload_to_postgres = PythonOperator(
    task_id='upload_to_postgres',
    python_callable=upload_to_postgres,
    op_args=['http://158.160.116.58:4009/download/', 'campaign_table', PS_CLIENT],
    dag=dag,
)

fetch_data_to_xcom >> upload_to_postgres
