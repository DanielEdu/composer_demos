import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from microservice.transform import JsonSplitter, equipments_tables, normalize_equipments
from airflow import models
import pendulum
import dateutil.parser
import requests 
import datetime

tzinfo = pendulum.timezone("America/Lima")

class LoadSqlOperator(BaseOperator):
    @apply_defaults
    def __init__(self, include_timestamp: bool=True, 
                    postgres_conn_id: str="postgres_ods_dev", 
                    database: str="ODS", 
                    data_key: str="entity_snapshots",
                    chunksize: int=1000,
                    *args,
                    **kwargs):
        BaseOperator.__init__(self, *args, **kwargs)
        self.include_timestamp = include_timestamp
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.data_key = data_key
        self.chunksize = chunksize

    def execute(self, context):
        ti = context["ti"]
        URL = ti.xcom_pull(key='location', task_ids='settings')
        print('Inicia desde -> ' + URL)

        data_row = requests.get(url = URL, timeout=None)
        print("  Estatus: " + str(data_row.status_code))
        print("data Row!")
        data_text = data_row.text
        print("data Text!")
        data_clean = data_text.replace( "'", '"' )
        print("data clean!")


        my_data = {
        'entity':'LOCATION',
        'data': data_clean
        }


        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        engine = self.hook.get_sqlalchemy_engine()

        dataframe = pd.DataFrame(my_data, index=[0])
        table_name, schema = 'entity_snapshots', 'fbplatform'

        
        if self.include_timestamp:
            ts = context["ts"]
            print('TS> '+ ts)
            ts = dateutil.parser.parse(ts) + datetime.timedelta(days=1)
            timestamp = tzinfo.convert(ts).isoformat("T","seconds")
            self.log.info(f"data timestamp: {timestamp}")
            dataframe = dataframe.assign(data_timestamp=timestamp)

        print('realizando insert a BD: ')
        dataframe.to_sql(table_name, schema=schema, con=engine, if_exists='append', index=False, chunksize=self.chunksize)
        self.log.info(f"{dataframe.shape[0]} rows inserted in {schema}.{table_name} table")

