import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow import models
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta
from ODS_batch.LoadSqlOperator import LoadSqlOperator


import os
import logging
import requests 


tz = pendulum.timezone('America/Lima')
SP_Create_Assets = models.Variable.get('Create_Assets_in_DB')

def settings(**context):
        ti = context["ti"]
        url_location = models.Variable.get('url_location')
        query_projects = models.Variable.get('query_projects')
        query_workorders = models.Variable.get('query_workorders')

        ti.xcom_push(key="location", value=url_location)
        ti.xcom_push(key="projects", value=query_projects)
        ti.xcom_push(key="workorders", value=query_workorders)


def dummy(**context):
    print("Dummyyyyyy")



dag = DAG(
    dag_id='ODS_batch_load',
	start_date=tz.convert(days_ago(1)),
    max_active_runs=1,
       # dagrun_timeout=datetime.timedelta(minutes=5),
	schedule_interval='0 23 * * *',
    dagrun_timeout=timedelta(minutes=90))



transform_task = PythonOperator(
    task_id='settings',
    python_callable=settings,
    dag=dag,
    provide_context=True
    )


load_Location = LoadSqlOperator(
    task_id="load_Location", 
    data_key='entity_snapshots', 
    execution_timeout=timedelta(minutes=90),
    dag=dag, 
    retries=1
    )

load_Projects = LoadSqlOperator(
    task_id='load_Proyects',
    python_callable=dummy,
    dag=dag,
    provide_context=True
    )

load_WorkOrders = LoadSqlOperator(
    task_id='load_WorkOrders',
    python_callable=dummy,
    dag=dag,
    provide_context=True
    )

# -------------------


CreateAssetsDB = PostgresOperator(
    task_id='CreateAssetsDB',
    postgres_conn_id='postgres_ods',
    sql=SP_Create_Assets,
    dag=dag,
    provide_context=True
    )

# ------------------------------------------------

PopulateIncidences = PostgresOperator(
    task_id='PopulateIncidences',
    postgres_conn_id='postgres_ods',
    sql='',
    dag=dag,
    provide_context=True
    )

PopulateCompliance = PostgresOperator(
    task_id='PopulateCompliance',
    postgres_conn_id='postgres_ods',
    sql='',
    dag=dag,
    provide_context=True
    )

PopulateEbcLocations = PostgresOperator(
    task_id='PopulateEbcLocations',
    postgres_conn_id='postgres_ods',
    sql='',
    dag=dag,
    provide_context=True
    )

PopulateCellLocations = PostgresOperator(
    task_id='PopulateCellLocations',
    postgres_conn_id='postgres_ods',
    sql='',
    dag=dag,
    provide_context=True
    )

# ------

TruncateNOC = PostgresOperator(
    task_id='TruncateNOC',
    postgres_conn_id='postgres_ods',
    sql='',
    dag=dag,
    provide_context=True
    )

ExtractDataCellInfo = PostgresOperator(
    task_id='ExtractDataCellInfo',
    postgres_conn_id='postgres_ods',
    sql='',
    dag=dag,
    provide_context=True
    )


GenerateInsert = LoadSqlOperator(
    task_id='GenerateInsert',
    python_callable=dummy,
    dag=dag,
    provide_context=True
    )

InsertNOC = PostgresOperator(
    task_id='InsertNOC',
    postgres_conn_id='postgres_ods',
    sql='',
    dag=dag,
    provide_context=True
    )


transform_task >> [load_Location,load_Projects,load_WorkOrders] >> CreateAssetsDB 
CreateAssetsDB >> [PopulateIncidences,PopulateCompliance,PopulateEbcLocations,PopulateCellLocations]
PopulateCellLocations >> TruncateNOC >> ExtractDataCellInfo >> GenerateInsert >> InsertNOC