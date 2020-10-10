import datetime

import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow import models
import os
import logging
import requests 


from microservice.operators.load_sql_operator import LoadSqlOperator
from microservice.transform import JsonSplitter, equipments_tables, normalize_equipments
 


tz = pendulum.timezone('America/Lima')

def transform_data(**context):
        ti = context["ti"]
        URL = models.Variable.get('url_equipment')
        ti.xcom_push(key="url", value=URL)

default_args = { 'retries': 3,
                 'retry_delay': datetime.timedelta(minutes=10)
                 }

dag = DAG(
        dag_id='etl_equipment_dev',
	start_date=tz.convert(days_ago(1)),
        max_active_runs=1,
       # dagrun_timeout=datetime.timedelta(minutes=5),
	schedule_interval='0 23 * * *',
        default_args=default_args
        )



transform_task = PythonOperator(
                task_id='transform_json',
                python_callable=transform_data,
                dag=dag,
                provide_context=True,
                )

transform_task

for subtable in equipments_tables:
    subtable_name = subtable["name"]
    transform_task >> LoadSqlOperator(
            task_id=f"load_{subtable_name}", 
            data_key=subtable_name, 
            dag=dag, 
            retries=2
            )
