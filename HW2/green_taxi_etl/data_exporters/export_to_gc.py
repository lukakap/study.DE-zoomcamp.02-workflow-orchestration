from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import pyarrow as pa
import pyarrow.parquet as pq

import os


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/angular-sign-demo-405419-aee0cafa32be.json"
project_id = 'angular-sign-demo-405419'
bucket_name = 'mage-zoomcamp-lk-1'
object_key = 'ny_green_taxi_data.parquet'
table_name = 'ny_green_taxi_data'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    
    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
