# dags/validate_raw_data.py
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowPythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 21),
    'retries': 1
}

with DAG('validate_raw_data',
         default_args=default_args,
         schedule_interval=None,  # Manual triggers only
         description='Validates and optimizes raw GeoParquet files') as dag:
    
    datasets = ['cadastral', 'water_projects', 'wetlands']
    
    for dataset in datasets:
        validate_task = DataflowPythonOperator(
            task_id=f'validate_{dataset}',
            py_file='gs://landbrugsdata-raw-data/dataflow/validate_geometries.py',
            options={
                'dataset': dataset,
                'project': 'landbrugsdata-1',
                'input_bucket': 'landbrugsdata-raw-data',
                'output_bucket': 'landbrugsdata-processed-data',
                'region': 'europe-west1',
                'temp_location': 'gs://landbrugsdata-raw-data/temp'
            }
        )