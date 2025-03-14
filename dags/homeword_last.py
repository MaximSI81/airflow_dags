# Импортируем необходимые библиотеки
import requests as req  # для выполнения HTTP-запросов
import pandas as pd  # для обработки данных
from datetime import datetime, timedelta  # для работы с датами
import json  # для парсинга json
from clickhouse_driver import Client  # для подключения к ClickHouse
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


# путь к папке
my_folder = json.loads(Variable.get('exchange_rate_my'))

# Подключение по api
api_url = BaseHook.get_connection('exchange_rate')
url = api_url.host
api = api_url.password


# Функция для обработки ошибки и отправки сообщения
def on_failure_callback(context):
    send_message = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_646053956',
        chat_id='-1002389701534',
        text=(context["ti"].task_id, context["ti"].state, context["ti"].log_url, context["ti"].dag_id),
        dag=dag)
    return send_message.execute(context=context)



# Функция для извлечения данных с API курса валют и сохранения их в локальный файл
def extract_task(**kwargs):
    """
    Эта функция выгружает данные по валютам, используя GET-запрос,
    и сохраняет результат в локальный файл `s_file`.
    """
    resp = req.get(f"{kwargs['url']}&start_date={kwargs['date']}&end_date={kwargs['date']}")
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
        for k, v in json.load(f).get('quotes').get(f"{kwargs['date']}").items():
            rows.append((kwargs['date'], 'USD', k[3:], v))

    df = pd.DataFrame(rows, columns=['date', 'currency_source', 'currency', 'value'])
    df.to_csv(kwargs['csv_file'], encoding='utf-8', index=False)


# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(**kwargs):
    """
    Эта функция считывает CSV файл и добавляет данные в ClickHouse
    """
    # Чтение данных из CSV
    data_frame = pd.read_csv(kwargs['csv_file'])
    # Запись data frame в ClickHouse
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    ch_hook.execute('INSERT INTO currency_sql_my VALUES', data_frame.to_dict('records'))


default_args = {
    'start_date': datetime(2024, 1, 1),
    'end_date': datetime(2024, 1, 5),
    'schedule_interval': '@daily',
    'on_failure_callback': on_failure_callback  # Устанавливаем функцию для обработки ошибки
    }

dag = DAG(dag_id='646053956_last', default_args=default_args, tags=['646053956_last'], max_active_runs=1)

extract_task = PythonOperator(task_id='extract_task', python_callable=extract_task,
                              op_kwargs={'url': f"{url}?access_key={api}", 'date': '{{ ds }}',
                                         's_file': my_folder['s_file']},
                              dag=dag)

# Оператор для создания таблицы
create_table = ClickHouseOperator(
    task_id='create_table',
    sql=''' CREATE TABLE IF NOT EXISTS currency_sql_my (`date` String, currency_source String, currency String, value Float64) ENGINE Log ''',
    clickhouse_conn_id='clickhouse_default',  # ID подключения, настроенное в Airflow
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task', python_callable=transform_data,
    op_kwargs={'s_file': my_folder['s_file'], 'csv_file': my_folder['csv_file'], 'date': '{{ ds }}'},
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_task', python_callable=upload_to_clickhouse,
    op_kwargs={'csv_file': my_folder['csv_file']}, dag=dag
)

create_new_order_table = ClickHouseOperator(
        task_id='create_new_order_table',
        sql=''' CREATE TABLE IF NOT EXISTS order_usd_my (date String, order_id Int64, purchase_rub Float64, purchase_usd Float64) ENGINE Log''',
        clickhouse_conn_id='clickhouse_default',
        dag=dag
)
insert_join = ClickHouseOperator(
    task_id='insert_join',
    sql=''' INSERT INTO order_usd_my
            SELECT c.date, o.order_id, o.purchase_rub, o.purchase_rub / c.value as purchase_usd
            FROM currency_sql_my c JOIN airflow.orders o using(date)
            WHERE c.currency = 'RUB' and o.date = '{{ ds }}' ''',
    clickhouse_conn_id='clickhouse_default',
    dag=dag
)


# Определение зависимостей между задачами
extract_task >> transform_task >> [create_table, create_new_order_table] >> upload_task >> insert_join