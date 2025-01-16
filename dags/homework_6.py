from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection('exchange_rate')

# Создаем DAG
dag = DAG(
    '646053956test',
    description='Пример использования ClickHouseOperator',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 4),
    user_defined_macros={'url': conn.host, 'api': conn.password}
)

# Оператор для создания таблицы
create_table = ClickHouseOperator(
    task_id='create_table',
    sql=''' CREATE TABLE IF NOT EXISTS currency_sql_my (`date` String, currency_source String, currency String, value float) ENGINE Log ''',
    clickhouse_conn_id='clickhouse_default',  # ID подключения, настроенное в Airflow
    dag=dag,
)

# SQL-запрос, вставка данных в нашу таблицу после того как мы их выгрузим из API
# Пришлось использовать такой запрос, Ваш выдавал ошибку
sql_query = """ INSERT INTO currency_sql_my

    SELECT '{{ ds }}' AS date, 'USD' AS currency_source,  RIGHT(col1, 3) AS currency, col2 AS value 
    FROM  
        (SELECT mapKeys(quotes['{{ ds }}']) AS col1, mapValues(quotes['{{ ds }}']) AS col2
            FROM url('{{ url }}?access_key={{ api }}&source=USD&start_date={{ ds }}&end_date={{ ds }}', JSONEachRow)
        ) LEFT ARRAY JOIN *;

"""

# Оператор для вствыки данных в таблицу
insert_data = ClickHouseOperator(
    task_id='insert_data',
    sql=sql_query,  # SQL запрос, который нужно выполнить
    clickhouse_conn_id='clickhouse_default',  # ID подключения, настроенное в Airflow
    dag=dag,
)

create_table >> insert_data