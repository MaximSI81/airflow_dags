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
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.telegram.operators.telegram import TelegramOperator


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
    'owner': '@max2',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'schedule_interval': None,
    'on_failure_callback': on_failure_callback,
}

conn = BaseHook.get_connection('psql_connection')
HOST = conn.host
USER = conn.login
SCHEMA = conn.schema
PASSWORD = conn.password
PORT = conn.port

engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{SCHEMA}')

dag = DAG(dag_id='dag_2',
          default_args=default_args,
          tags=['dag_2']
          )


def extract_csv_to_postgres(path: str, **context):
    df = pd.read_csv(path)
    df.to_sql(
        name="products_dixi",  # имя таблицы
        con=engine,  # движок
        if_exists="append",  # если таблица уже существует, добавляем
        index=False  # без индекса
    )


extract_csv_to_postgres = PythonOperator(
    task_id='extract_csv_to_postgres',
    python_callable=extract_csv_to_postgres,
    op_kwargs={'path': '/opt/airflow/dags/part-00000-4aca0172-df77-427f-8444-bc7207b0c13e-c000.csv'},
    dag=dag
)

create_table_dixi = PostgresOperator(
    task_id='create_table',
    sql=''' CREATE TABLE IF NOT EXISTS products_dixi (Название text, "Тип продукта" text, Бренд  text, price text, date_price text); ''',
    postgres_conn_id='psql_connection',
    dag=dag
)

create_table_magn = PostgresOperator(
    task_id='create_table_magn',
    sql=''' CREATE TABLE IF NOT EXISTS products_magn (Название text, "Тип продукта" text, Бренд  text, price text, date_price text); ''',
    postgres_conn_id='psql_connection',
    dag=dag
)

join_table_postgres = PostgresOperator(
    task_id='join_table_postgres',
    sql=''' INSERT INTO  products_magn SELECT "Название", "Тип продукта", "Бренд", price, date_price FROM demo_public.products_magnit 
            WHERE date_price = 'date_ 2025-02-28 00:00:00' '''
)

[create_table_dixi, create_table_magn] >> extract_csv_to_postgres >> join_table_postgres
