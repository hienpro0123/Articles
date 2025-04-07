import os
import psycopg2
from psycopg2 import Error
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from crawl import crawl_data
from transform import transform_data
from saved_sql import insert_data_to_database

# Tạo DAG
dag = DAG(
    'insert_data',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 10 * * *',
    catchup=False
)

# Task crawl_data: Thu thập dữ liệu
def crawl_data_task(**kwargs):
    data = crawl_data()  # Thu thập dữ liệu từ crawl.py
    kwargs['ti'].xcom_push(key='crawl_data', value=data)  # Truyền dữ liệu vào XCom

crawl_task = PythonOperator(
    task_id='crawl_data',
    python_callable=crawl_data_task,
    dag=dag,
)

# Task transform_data: Xử lý dữ liệu
def transform_data_task(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='crawl_data', key='crawl_data')  # Lấy dữ liệu từ XCom
    if not data:
        raise ValueError("Dữ liệu thu thập được từ crawl_data rỗng hoặc không hợp lệ.")
    transformed_data = transform_data(data)  # Xử lý dữ liệu
    if isinstance(transformed_data, list) and isinstance(transformed_data[0], dict):
        return transformed_data
    else:
        raise ValueError("Dữ liệu trả về từ transform_data không đúng định dạng.")

trans_task = PythonOperator(
    task_id='trans_data',
    python_callable=transform_data_task,
    dag=dag,
)

# Task save_data: Lưu trữ vào cơ sở dữ liệu
def insert_data_task(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='trans_data') # Lấy dữ liệu đã được xử lý từ XCom
    if not data:
        print("Dữ liệu cần chèn vào cơ sở dữ liệu không có.")
        return
    insert_data_to_database(data)  # Chèn từng bài viết vào cơ sở dữ liệu
          

saved_task = PythonOperator(
    task_id='save_data',
    python_callable=insert_data_task,
    dag=dag,
)

# Định nghĩa thứ tự thực thi các task
crawl_task >> trans_task >> saved_task
