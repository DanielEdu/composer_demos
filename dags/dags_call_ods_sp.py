import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from common import tzinfo


procedures = {'incidence_projects_creation' : '''call ipt.incidence_projects_creation( CAST( to_char(CURRENT_TIMESTAMP,'YYYY-MM-DD HH24:MI:SS') as TIMESTAMP ) )''',
              'gestion_tickets_projects_creation' : '''call ipt.gestion_tickets_projects_creation( CAST( to_char(CURRENT_TIMESTAMP,'YYYY-MM-DD HH24:MI:SS') as TIMESTAMP ) )'''}


default_args = {
    'start_date': tzinfo.convert(datetime.datetime(2020, 9, 28)),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

with DAG(
    'call_ods_sp',
    default_args=default_args,
    description='call store procedures in ODS',
    max_active_runs=1,
    catchup=False,
    schedule_interval='*/30 0-22 * * *',
    dagrun_timeout=datetime.timedelta(minutes=20)) as dag:

    for name, query in procedures.items():
         PostgresOperator(
                task_id=name,
                postgres_conn_id='postgres_ods',
                sql=query,
                provide_context=True,
                )

