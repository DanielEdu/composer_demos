import json
from typing import List, Tuple, Dict, Callable
from datetime import datetime

import pandas as pd



class TableInfo():
    def __init__(self, name: str,
                       schema: str,
                       column_map: List[Tuple[str, str]]) -> None:
        self._name = name
        self._schema = schema
        self._column_map = column_map
        self._columns = list(list(zip(*column_map))[1])

    @property
    def schema(self):
        return self._schema

    @property
    def columns(self):
        return self._columns

    @property
    def name(self):
        return self._name

    @property
    def column_map(self):
        return dict(self._column_map)

    def selection(self, super_table: pd.DataFrame) -> dict:
        data = super_table.rename(columns=self.column_map)[self.columns]\
                           .drop_duplicates()\
                           .reset_index(drop=True)\
                           .to_dict(orient='list')
        return {'name': self.name,
                'schema': self.schema,
                'data': data}


class JsonSplitter():
    def __init__(self, json_data: dict,
                       normalize_function: Callable,
                       tables_info: List[dict]) -> None:
        self.json_data = json_data
        self.tables_info = tables_info
        self.flat_table = normalize_function(json_data)

    @classmethod
    def from_file(cls, filename: str, *args):
        with open(filename) as file:
            json_data = json.load(file)
            return cls(json_data, *args)

    @classmethod
    def from_string(cls, input_str: str, *args):
        json_data = json.loads(input_str)
        return cls(json_data, *args)

    def split(self):
        return [TableInfo(**info).selection(self.flat_table) for info in self.tables_info]


def normalize_equipments(json_data: List[dict]) -> pd.DataFrame:
    series_data = pd.Series(json_data)
    required_fields = ['parentLocation', 'properties', 'equipmentType']
    not_null = series_data.apply(lambda x: all(map(lambda y: x.get(y), required_fields)))
    series_data = series_data[not_null]
    flat_df = pd.io.json.json_normalize(series_data, 'properties',
                  ['id', 'name', 'externalId',
                   ['parentLocation', 'id'],
                   ['equipmentType', 'id'],
                   ['equipmentType', 'name']],
                                record_prefix='properties.',
                                meta_prefix='equipments.',
                                errors='ignore')
    return flat_df


SCHEMA = 'fbplatform'
equipments = {'name':'equipments',
              'schema' : SCHEMA,
              'column_map': [('equipments.id', 'id'),
                             ('equipments.name', 'name'),
                             ('equipments.externalId', 'external_id'),
                             ('equipments.parentLocation.id', 'parentlocation_id'),
                             ('equipments.equipmentType.id', 'equipmenttype_id'),
                             ('equipments.equipmentType.name', 'equipmenttype_name')]}

equipments_properties = {'name' : 'equipments_properties',
                         'schema' : SCHEMA,
                         'column_map': [('equipments.id', 'equipment_id'),
                                        ('properties.propertyType.name', 'property_type_name'),
                                        ('properties.stringValue', 'stringvalue')]}
equipments_tables = [equipments, equipments_properties]


data_timestamp=datetime.now()
def load_postgres(name: str, schema: str, data: List[dict], include_timestamp: bool=True):
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://edgar.jesus.fuentes.echezuria%40everis.com:vhBaUuY7Fxne68ck@35.196.68.89:5432/ODS')
    dataframe = pd.DataFrame(data)
    if include_timestamp:
        dataframe = dataframe.assign(data_timestamp=data_timestamp)
    #dataframe = dataframe.loc[:100]
    print(dataframe.shape)
    dataframe.to_sql(name, schema=schema, con=engine, if_exists='append', index=False, chunksize=1000)


#js = JsonSplitter.from_file("all.json", normalize_equipments, equipments_tables )
#js.split()
