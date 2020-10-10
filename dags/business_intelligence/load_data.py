import os
import re
from collections import OrderedDict

from google.cloud import bigquery
import pandas as pd
import pandas_gbq


from common import current_datetime, get_credentials, TZ

def valid_name(name):
    name = name.strip().lower()\
               .replace('%', 'perc')\
               .replace('año', 'anio')
    if name.startswith('4g'):
        name = name.replace('4g', 'lte', 1)
    clean_re = re.compile('[^a-z0-9]+')
    name = re.sub(clean_re, '_', name).strip('_')
    return name


table_types =  { 'pw':    {  'dataset': 'BI_landing_zone', 
                             'tablename': 'IPT_New_KPIs_per_Cell',
                             'prefix' : 'IPT+New+KPIs+per+Cell',
                             'sep': ',',
                             'na_values': ['--'],
                             'date_col': 'date_hour',
                             'date_format': '%m/%d/%Y %H:%M', #'09/03/2020 20:00',
                             'rename_cols': {'Node Name _________ (CWS name)': 'node_name'},
                             'float_cols' : ['srvcc_sr_perc'],
                             'bq_procedure': ['LeanBI_DEV.procesar_archivos_trafico_diario_pw']
                             },
               'tdp':     {  'dataset': 'BI_landing_zone',
                              'tablename': 't_calidad_kpis_formula_tdp',
                              'prefix': 't_calidad_kpis_formula_tdp',
                              'sep': ';',
                              'na_values': ['NULL'],
                              'date_col': 'dia',
                              'date_format': '%d/%m/%Y %H', #'24/08/2020 00',
                              'bq_procedure': ['LeanBI_DEV.procesar_archivos_trafico_diario_tdp']
                            },
               'tdp_2g_3g_4g': { 'dataset': 'BI_landing_zone',
                              'tablename': 'kpis_tdp_semanal_2g_3g_4g',
                              'prefix': 'kpis_tdp_semanal_2G_3G_4G',
                              'sep': ';',
                              'na_values': ['NULL'],
                              'date_col': 'fecha_semana',
                              'date_format': '%Y/%m/%d',  #'2020-08-24',
                              'bq_procedure': ['']
                            },
               'tdp_3g':  { 'dataset': 'BI_landing_zone',
                              'tablename': 'kpis_tdp_semanal_3g',
                              'prefix': 'kpis_tdp_semanal_3G',
                              'sep': ';',
                              'na_values': ['NULL'],
                              'date_col': 'fecha_semana',
                              'date_format': '%Y/%m/%d',  #'2020-08-24',
                              'bq_procedure': ['']
                            },
               'tdp_4g':    {'dataset': 'BI_landing_zone',
                              'tablename': 'kpis_tdp_semanal_4g',
                              'prefix': 'kpis_tdp_semanal_4G',
                              'sep': ';',
                              'na_values': ['NULL'],
                              'date_col': 'fecha_semana',
                              'date_format': '%Y/%m/%d',  #'2020-08-24',
                              'bq_procedure': ['']
                              
                            }
                }         

names = list(table_types.keys())

bq_procedures = {name:table['bq_procedure']for name, table in table_types.items()}

def get_groups(field='prefix'):
    group_fields = {}
    for group, tables in table_types.items():
        group_fields[group] = list(params.get(field) for params in tables)
    return group_fields


def names_match(objects):
    file_objects = list(zip(map(os.path.basename, objects), objects))
    matched_files = []
    for name, definition in table_types.items():
        prefix = definition['prefix']
        prefixed_files = list(filter(lambda x: x[0].startswith(prefix), file_objects))
        if len(prefixed_files) >= 1:
            matched_files.append((name, *prefixed_files[:1][0]))
    if matched_files:
        names, files, objects = zip(*matched_files)
        return names, files, objects
    return None, None, None


class FileValidator():
    def __init__(self, filepath):
        self.filepath = filepath
        self.filename = os.path.basename(filepath)

    def is_valid(self):
        for table in table_types.values():
            prefix = table['prefix']
            if self.filename.startswith(prefix):
                sep, na_values = table.get('sep', ';'), table.get('na_values', ['NULL'])
                try:
                    self.data = pd.read_csv(self.filepath, sep=sep, na_values=na_values)
                except UnicodeDecodeError:
                    self.data = pd.read_csv(self.filepath, sep=sep, na_values=na_values, encoding='latin')
                return table, self.data
        return None, None

def to_int(column):
    try:
        return column.astype('Int64', errors='ignore')
    except TypeError:
        return column

class TableInfo():

    def __init__(self, data: pd.DataFrame, data_timestamp, dataset: str, tablename: str,  
            date_col: str, date_format=None, rename_cols: dict={}, 
            float_cols:list=[], **kwargs):
        self.data = data
        self.data_timestamp = data_timestamp
        self.dataset = dataset
        self.tablename = tablename
        self.date_col = date_col
        self.date_format = date_format
        self.rename_cols = rename_cols
        self.float_cols = float_cols
        self.clean()
    
    def clean(self):
        timestamp_col = 'data_timestamp'
        date_cols = [self.date_col, timestamp_col]
        self.data[timestamp_col] = self.data_timestamp
        fields_map = {name:valid_name(name) for name in self.data.columns}
        rename = {**fields_map, **self.rename_cols}
        self.data.rename(columns=rename, inplace=True)
        self.data.loc[:, self.date_col] = self.data[self.date_col].apply(lambda x: pd.to_datetime(x, format=self.date_format).tz_localize(TZ))
        self.data.loc[:, timestamp_col] = self.data[timestamp_col].apply(lambda x: pd.to_datetime(x))
        int_cols = list(set(self.data.columns)-set([timestamp_col, self.date_col]))
        self.data.loc[:, int_cols] = self.data[int_cols].apply(to_int)
        if self.float_cols:
            self.data.loc[:, self.float_cols] = self.data[self.float_cols].astype(float) 
        initial_cols = list(self.data.columns)
        initial_cols.remove(timestamp_col)
        self.data = self.data[[timestamp_col]+initial_cols]

    def to_bigquery(self):
        tablename = f'{self.dataset}.{self.tablename}'
        credential, project_id = get_credentials()
        self.data.to_gbq(tablename, credentials=credential, if_exists='append', progress_bar=False)


class InvalidSchemaError(Exception):
    def __init__(self, schema):
        msg = f'this file contain an invalid schema: {schema}'
        super(InvalidSchemaError, self).__init__(msg)


class UndefinedFileError(Exception):
    def __init__(self, filename):
        msg = f'this file does\'t match with valid definitions: {filename}'
        super(UndefinedFileError, self).__init__(msg)


def load_bigquery(filename, data_timestamp, **context):
    table_params, data = FileValidator(filename).is_valid()
    error_msg = None
    if table_params:
        print(data_timestamp)
        print(table_params)
        try:
            table_info = TableInfo(**table_params, data=data, data_timestamp=data_timestamp)
            print(table_info.data.info())
            table_info.to_bigquery()
        except pandas_gbq.gbq.InvalidSchema as e:
            data_types = list(tuple(map(str, item)) for item in data.dtypes.to_dict().items())
            raise InvalidSchemaError(data_types)
    else:
        raise UndefinedFileError(filename)
