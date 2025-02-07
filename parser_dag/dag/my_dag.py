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
from parser_dag.script.parser_dixi import ParserDixi
from parser_dag.script.parser_magnit import ParserProducts
from airflow.hooks.postgres_hook import PostgresHook

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
    products = ['17591-sosiski_kolbasy_delikatesy', '4834-moloko_syr_yaytsa', '4884-ovoshchi_frukty',
                '4855-myaso_ptitsa_kolbasy', '5276-chay_kofe_kakao', '5011-sladosti_torty_pirozhnye',
                '5269-khleb_vypechka_sneki']
    data = []
    for i in products:
        p = ParserProducts(f'https://magnit.ru/catalog/{i}?shopCode=503051&shopType=1')
        p()
        data += p.products
    kwargs['ti'].xcom_push(key='data', value=data)


def get_df(**kwargs):
    name_column = ['Название', 'Тип продукта', 'Бренд', 'price', 'date_price']
    df = pd.DataFrame(columns=name_column)
    x = 0
    for i in kwargs['ti'].xcom_pull(key='key_name'):
        data_list = transformation([','.join(i)], name_column)
        if data_list:
            try:
                df.loc[x] = transformation([','.join(i)], name_column)[:-1]
                x += 1
            except ValueError:
                pass
    return df


def parser_d():
    prod = ['molochnye-produkty-yaytsa/', 'konditerskie-izdeliya-torty/', 'ovoshchi-frukty/', 'khleb-i-vypechka/',
            'myaso-ptitsa/', 'myasnaya-gastronomiya/', 'chay-kofe-kakao/']
    url = 'https://dixy.ru/catalog/'
    data = []
    for p in prod:
        PD = ParserDixi(url, [p])
        PD.get_prod()
        data += PD.prod_inf
    return data


def postgres_upload(df):
    pg_hook = PostgresHook(postgres_conn_id='postgres3')
    pg_hook.execute('INSERT INTO currency_sql_my VALUES', df.to_dict('records'))


parser_m = PythonOperator(task_id='parser_m', python_callable=parser_m, dag=dag)
get_df = PythonOperator(task_id='get_df', python_callable=get_df, dag=dag)
create_table = PostgresOperator(
    task_id='create_table',
    sql=''' CREATE TABLE IF NOT EXISTS products_magnit ('Название' varchar(200), 'Тип продукта' varchar(50), 'Бренд'  varchar(50), 'price' float, 'date_price' timestamp; ''',
    postgres_conn_id='postgres3',
    dag=dag
)
postgres_upload = PythonOperator(task_id='postgres_upload', python_callable=postgres_upload, dag=dag)


parser_m >> get_df >> create_table >> postgres_upload