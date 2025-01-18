from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import json


dag = DAG(
    dag_id='646053956',
    schedule_interval=None,  # Запускать вручную
    start_date=days_ago(1)
)


def response_operator(response, **kwargs):
    if 'report_id' in response.json():
        kwargs['ti'].xcom_push(key='get_report', value=response.json()['report_id'])
        return True


def response_sensor(response, **kwargs):
    status = response.json()["message"]
    if status == "The report is still being prepared...":
        pass
    elif status == "The report is ready!":
        return True
    return False


# HTTP-оператор для отправки запроса на создание отчёта
start_report_task = SimpleHttpOperator(
    task_id='start_report_task',
    http_conn_id='my_conn',
    endpoint='start_report',
    method='GET',
    response_check=response_operator,  # Проверка на наличие Report ID
    dag=dag,
)

# HTTP-сенсор для проверки готовности отчёта
check_report_task = HttpSensor(
    task_id='check_report_task',
    http_conn_id='my_conn',  # Укажите ваше соединение
    endpoint='check_report/' + "{{ ti.xcom_pull(key='get_report') }}",
    response_check=response_sensor,         # Проверка что отчет готов
    method='GET',
    poke_interval=2,
    timeout=60,
    mode='poke',
    dag=dag,
)

# Определение порядка выполнения задач
start_report_task >> check_report_task
