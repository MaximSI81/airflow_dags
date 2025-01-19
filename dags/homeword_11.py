from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
import pandas as pd


class ClickhouseTransferHook(ClickHouseHook):  # Наследуемся от ClickHouseHook

    def get_pandas_df(self, url_or_path):
        """ Ваш код который читает данные из файла
        """
        # Ваш код который читает и возвращает данные из CSV файла в pandas Data Frame
        # Используйте обычный pandas
        return pd.read_csv(url_or_path)

    def insert_df_to_db(self, data_frame, table_name):
        """ Данный метод вставляет Data Frame в ClickHouse
        """
        # Объект ClickHouseHook может используя подключение к бд выполнить SQL код
        ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
        ch_hook.execute(f'INSERT INTO {table_name} VALUES', data_frame.to_dict('records'))


class ClickhouseTransferOperator(BaseOperator):

    def __init__(self, path, table_name, **kwargs):
        super().__init__(**kwargs)
        self.hook = None
        self.path = path  # Путь до файла
        self.table_name = table_name  # Имя таблицы

    def execute(self, context):
        # Создание объекта хука
        self.hook = ClickhouseTransferHook()
        self.hook.insert_df_to_db(self.hook.get_pandas_df(self.path), self.table_name)
        # Ваш код вызовите метод который
        # читает данные и затем записывает данные в БД
        # Нужно использовать self.hook для доступа к методам ниже
        # get_pandas_df и insert_df_to_db


# DAG
dag = DAG(dag_id='646053956_custom', schedule_interval=None, start_date=days_ago(1))

# Оператор для создания таблицы
create_table = ClickHouseOperator(
    task_id='create_table',
    sql='CREATE TABLE IF NOT EXISTS campaign_table_my (campaign String, cost Int64, date  String) ENGINE Log',
    clickhouse_conn_id='clickhouse_default',
    dag=dag,
)

# Оператор для трансфера данных
transfer_data = ClickhouseTransferOperator(
    task_id='transfer_data',
    path='http://158.160.116.58:4009/download/file1.txt',
    table_name='campaign_table_my',
    dag=dag)

create_table >> transfer_data
