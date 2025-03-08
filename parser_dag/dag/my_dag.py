from airflow.providers.telegram.operators.telegram import TelegramOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine, text
import pandas as pd
from airflow.utils.dates import days_ago
import csv
import requests
from airflow.exceptions import AirflowException
from sqlalchemy import Table, Column, Integer, String, MetaData, VARCHAR
from airflow.hooks.base_hook import BaseHook
from parser_magnit import ParserProducts
from airflow.hooks.postgres_hook import PostgresHook


# Функция для обработки ошибки и отправки сообщения
def on_failure_callback(context):
    send_message = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_id',
        chat_id='-1002389701534',
        text=(context["ti"].task_id, context["ti"].state, context["ti"].log_url, context["ti"].dag_id),
        dag=dag)
    return send_message.execute(context=context)


default_args = {
    'owner': '@max',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'on_failure_callback': on_failure_callback,
    'schedule_interval': '@daily',
    'retries': 3,  # attempts
    'retry_delay': timedelta(minutes=10),  # interval
}

conn = BaseHook.get_connection('psql_connection')
HOST = conn.host
USER = conn.login
SCHEMA = conn.schema
PASSWORD = conn.password
PORT = conn.port

engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{SCHEMA}')

dag = DAG('dag1', default_args=default_args,
          tags=['parser_dag'])


def transformation(description_list, name_column):
    data = []
    for i in name_column:
        for j in description_list[0].split(','):
            if i in j:
                data.append(j.replace(i, '').lstrip())
    return data


def parser_m(**kwargs):
    prod = ['17591-sosiski_kolbasy_delikatesy', '4834-moloko_syr_yaytsa', '4884-ovoshchi_frukty',
            '4855-myaso_ptitsa_kolbasy', '5276-chay_kofe_kakao', '5011-sladosti_torty_pirozhnye',
            '5269-khleb_vypechka_sneki']
    data = []
    for i in prod:
        p = ParserProducts(f'https://magnit.ru/catalog/{i}?shopCode=503051&shopType=1')
        p()
        data += p.products
    name_column = ['Название', 'Тип продукта', 'Бренд', 'price', 'date_price']
    patch_file = '/tmp/data.csv'
    kwargs['ti'].xcom_push(key='data', value=patch_file)
    df = pd.DataFrame(columns=name_column)
    x = 0
    for i in data:
        data_list = transformation([','.join(i)], name_column)
        if data_list:
            try:
                df.loc[x] = transformation([','.join(i)], name_column)[:-1]
                x += 1
            except ValueError:
                pass
    df.to_csv(patch_file, index=False, encoding='utf-8')


def postgres_upload(**kwargs):
    patch_file = kwargs['ti'].xcom_pull(key='data')
    df = pd.read_csv(patch_file)
    df.to_sql(
        name="products_magnit",  # имя таблицы
        con=engine,  # движок
        if_exists="append",  # если таблица уже существует, добавляем
        index=False  # без индекса
    )


parser_m = PythonOperator(task_id='parser_m', python_callable=parser_m, dag=dag)
create_table = PostgresOperator(
    task_id='create_table',
    sql=''' CREATE TABLE IF NOT EXISTS products_magnit (Название text, "Тип продукта" text, Бренд  text, price text, date_price text); ''',
    postgres_conn_id='psql_connection',
    dag=dag
)
postgres_upload = PythonOperator(task_id='postgres_upload', python_callable=postgres_upload, dag=dag)

parser_m >> create_table >> postgres_upload