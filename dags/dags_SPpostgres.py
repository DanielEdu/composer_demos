import pendulum
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import models
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.postgres_operator import PostgresOperator


tz = pendulum.timezone('America/Lima')
SP = models.Variable.get('querySP')

print(SP)




default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'PostgresSP_dag',
    default_args=default_args,
    description='call ipt.incidence_projects_creation',
    max_active_runs=1,
    schedule_interval='*/30 0-22 * * *',
    dagrun_timeout=timedelta(minutes=20))



executeSP = PostgresOperator(
                task_id='executeSP',
                postgres_conn_id='postgres_ods',
                sql=SP,
                dag=dag,
                provide_context=True,
                )

#OITNB=#TWD

executeSP
