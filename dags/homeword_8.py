from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from datetime import datetime


# Функция для обработки ошибки и отправки сообщения
def on_failure_callback(context):
    send_message = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_646053956',
        chat_id='-1002389701534',
        text='The task has failed!',
        dag=dag)
    return send_message.execute(context=context)


# Данная функция вызовет исключение
def raise_exc():
    raise AirflowException


default_args = {
    'on_failure_callback': on_failure_callback  # Устанавливаем функцию для обработки ошибки
}

dag = DAG(
    dag_id='646053956_telegram',
    default_args=default_args,
    schedule_interval=None,  # Только ручной запуск
    start_date=datetime(2023, 1, 1)
)

python_task = PythonOperator(
    task_id='raise_exception_task',
    python_callable=raise_exc,
    dag=dag
)

python_task