import datetime
import dateutil.parser

import pendulum
from airflow import DAG
from airflow.models import Variable

from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.bash_operator import BashOperator

from common import tzinfo, save_result, read_result
from tolerance.query_dates import missing_intervals, DATE_FORMAT
from tolerance.sensors.reverse_sftp_sensor import ReverseSFTPSensor


def make_query(**context):
    window_size = int(Variable.get('tolerance_window_size', 1))
    ti = context['ti']
    ts = context['ts']
    end_date = tzinfo.convert(dateutil.parser.parse(ts)).date()
    start_date = (end_date - datetime.timedelta(days=window_size)).strftime(DATE_FORMAT)
    end_date = end_date.strftime(DATE_FORMAT)
    ti.log.info(f'start: {start_date}, end: {end_date}')
    missing = missing_intervals(start_date, end_date)
    ti.log.info(f'{missing}')
    save_result(context, missing, extension='json')


default_args = {
    'owner': 'edgar',
    'max_active_runs': 1,
    'start_date': tzinfo.convert(datetime.datetime(2020, 8, 16)),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5), 
    }

with DAG(
        dag_id='tolerance',
        schedule_interval='0 1 * * *',
        default_args=default_args,
        catchup=False) as dag:
    
    query_task = PythonOperator(
                task_id='make_query',
                python_callable=make_query,
                retries=2,
                retry_delay=datetime.timedelta(minutes=1),
                provide_context=True,
                )

    put_task = SFTPOperator(
                task_id='put_sftp_nifi',
                ssh_conn_id='ssh_nifi_prod',
                local_filepath=f'/home/airflow/gcs/data/{dag.dag_id}/make_query.json',
                remote_filepath='/tmp/make_query.json',
                retries=2,
                retry_delay=datetime.timedelta(minutes=10),
                operation='put',
                create_intermediate_dirs=True
                )
    
    verify_task = ReverseSFTPSensor(
                task_id='is_processed',
                path='/tmp/make_query.json',
                sftp_conn_id='ssh_nifi_prod',
                poke_interval=60*5,
                mode='reschedule'
                )

    query_task >> put_task >> verify_task


