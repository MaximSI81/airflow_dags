from airflow import DAG
from airflow.utils.dates import days_ago
import os
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

dag = DAG('646053956_data_marts', schedule_interval=None, start_date=days_ago(1), tags=['data_marts'])

# Этот код поможет вам получить доступ к файлам по котоырм вам нужно пройтись

# Путь к папке с файлами
folder_path = "/usr/local/airflow/plugins/sql/"
tasks = []

# Проходимся по файлам и генерим задачи
for file_name in os.listdir(folder_path):
    with open(folder_path + file_name) as f:
        tasks.append(ClickHouseOperator(task_id=f'task_{file_name[3]}',
                                        sql=f"CREATE VIEW data_mart_{file_name[:-4]} as {f.read()}",
                                        clickhouse_conn_id='clickhouse_default', dag=dag))

for i in range(len(tasks)):

    if i > 0:
        tasks[i - 1] >> tasks[i]
