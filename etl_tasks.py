# etl_tasks.py

import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os

def extract_data_from_sql():
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_default')
    sql = "SELECT * FROM test_table"  
    df = mssql_hook.get_pandas_df(sql)
    df.to_csv('/tmp/extracted_data2.csv', index=False)  

def load_data_to_gcs():
    key_file_path = '/home/airflow/gcs/dags/stunning-ruler-427604-p9-509c42812e4a.json'
    if not os.path.isfile(key_file_path):
        raise FileNotFoundError(f"Key file not found: {key_file_path}")

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_storage_default')
    gcs_hook.upload(
        bucket_name='test-bucket-112233',  
        object_name='composer/extracted_data2.csv',  
        filename='/tmp/extracted_data2.csv'  
    )
