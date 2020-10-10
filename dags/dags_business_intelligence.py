import datetime
from collections import namedtuple
import os

import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator

from business_intelligence.sensors.newfile_sensor import NewFileSensor
from business_intelligence.load_data import load_bigquery, bq_procedures
from common import tzinfo, call_bigquery, DATA_PATH, current_datetime, get_datapath


SENSOR_ID = 'check_input_files'


def load_data(**context):
    ti = context['ti']
    file_key = ti.task_id.split('_')[1]
    filename = ti.xcom_pull(key=file_key, task_ids=SENSOR_ID)
    data_timestamp = ti.xcom_pull(key='data_timestamp', task_ids=SENSOR_ID)
    data_path = get_datapath(ti)
    filepath = os.path.join(data_path, filename)
    load_bigquery(filepath, data_timestamp, **context)


def select_load(**context):
    ti = context['ti']
    names = ti.xcom_pull(key='names', task_ids=SENSOR_ID)
    return [f'load_{name}_to_bq' for name in names]


default_args = {
    'owner': 'edgar',
    'start_date': tzinfo.convert(datetime.datetime(2020, 8, 16)),
    'depends_on_past': True,
    'wait_for_downstream': True
    }


with DAG(
        dag_id='business_intelligence',
        default_args=default_args,
        catchup=False,
        schedule_interval='*/5 7-20 * * *',
        #dagrun_timeout=datetime.timedelta(hours=24),
        max_active_runs=1) as dag:

    valid_prefixes = ['IPT+New+KPIs+per+Cell+(Hourly)_', 't_calidad_kpis_formula_tdp', 'kpis_tdp_semanal']
    list_task = NewFileSensor(
                task_id=SENSOR_ID,
                google_cloud_conn_id='gcs_ipt_prod',
                bucket='gcs-ipt-stackoss-business-intelligence',
                prefixes=[f'trafico_vendors/pendientes_procesar/{prefix}' for prefix in valid_prefixes],
                poke_interval=60,
                mode='reschedule',
                retries=3
                )

    selector_task = BranchPythonOperator(
        task_id='select_load',
        python_callable=select_load,
        provide_context=True)

    list_task >> selector_task
   
    delete_local_files = BashOperator(
                task_id='delete_local_files',
                bash_command=f'rm {DATA_PATH}/{dag.dag_id}/*',
                provide_context=True
                ) 

    move_processed_files = GoogleCloudStorageToGoogleCloudStorageOperator(
                task_id=f'move_processed_files',
                trigger_rule='none_failed',
                source_bucket='gcs-ipt-stackoss-business-intelligence',
                source_object='trafico_vendors/pendientes_procesar/*.csv',
                destination_bucket='gcs-ipt-stackoss-business-intelligence',
                destination_object='trafico_vendors/procesados/',
                move_object=True,
                google_cloud_storage_conn_id='gcs_ipt_prod',
                retries=2)

    for name, procedures in bq_procedures.items():
        load_task = PythonOperator(
                        task_id=f'load_{name}_to_bq',
                        python_callable=load_data,
                        retries=2,
                        provide_context=True
                        )
        selector_task >> load_task
        for procedure in procedures:
            call_procedure = PythonOperator(
                 task_id=f'call_{name}_procedure', 
                 python_callable=call_bigquery, 
                 op_kwargs={'sql_string':f'''DECLARE proc_timestamp TIMESTAMP DEFAULT '{{{{ ti.xcom_pull(key='data_timestamp', task_ids='{SENSOR_ID}') }}}}'; 
                                        CALL `ipt-internet-para-todos.{procedure}`(proc_timestamp);'''},
                 retries=2,
                 provide_context=True
                )
            load_task >> call_procedure >> move_processed_files >> delete_local_files

