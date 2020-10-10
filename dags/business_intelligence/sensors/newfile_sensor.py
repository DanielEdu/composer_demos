import os

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.decorators import apply_defaults
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor

from common import create_datapath, current_datetime
from business_intelligence.load_data import names_match

class NewFileSensor(GoogleCloudStoragePrefixSensor):
    """
    Checks for the existence of a objects at prefix in Google Cloud Storage bucket.

    :param bucket: The Google cloud storage bucket where the object is.
    :type bucket: str
    :param prefix: The name of the prefix to check in the Google cloud
        storage bucket.
    :type prefix: str
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """
    template_fields = ('bucket',)

    ui_color = '#f0eee4'


    @apply_defaults
    def __init__(self, prefixes: list=['*'], *args, **kwargs):
        GoogleCloudStoragePrefixSensor.__init__(self, prefix='*', *args, **kwargs)
        self.prefixes = prefixes

    def __download(self, hook, objects, files, ti):
        for obj, fil in zip(objects, files):
            data_path = create_datapath(ti)
            filename = f'{data_path}/{fil}'
            hook.download(self.bucket, obj, filename=filename)


    def poke(self, context):
        self.log.info('Sensor checks existence of objects: %s, %s',
               self.bucket, self.prefix)
        hook = GoogleCloudStorageHook(
               google_cloud_storage_conn_id=self.google_cloud_conn_id,
               delegate_to=self.delegate_to)
        objects = []
        for prefix in self.prefixes:
            objects.extend(list(hook.list(self.bucket, prefix=prefix)))
        self.log.info(f'Objects list: {objects}')
        names, files, objects = names_match(objects)
        if names:
            ti = context['ti']
            self.__download(hook, objects, files, ti)
            ti.xcom_push(key='names', value=names)
            ti.xcom_push(key='files', value=files)
            ti.xcom_push(key='objects', value=objects)
            for name, fil in zip(names, files):
                ti.xcom_push(key=f'{name}', value=fil)
            self.log.info(f'names: {names}\nfiles: {files}\nobjects: {objects}')
            data_timestamp = current_datetime().isoformat()
            ti.xcom_push(key='data_timestamp', value=data_timestamp)
            return True
        return False

