from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowPythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 21),
    'retries': 1
}

with DAG('data_processing',
         default_args=default_args,
         schedule_interval=None,  # Manual triggers only
         description='Data processing workflows for Landbrugsdata') as dag:
    
    # Validation jobs
    datasets = ['cadastral', 'water_projects', 'wetlands']
    validation_tasks = []
    
    for dataset in datasets:
        validate_task = DataflowPythonOperator(
            task_id=f'validate_{dataset}',
            py_file='gs://landbrugsdata-processing/dataflow/validate_geometries.py',
            options={
                'dataset': dataset,
                'project': 'landbrugsdata-1',
                'input_bucket': 'landbrugsdata-raw-data',
                'output_bucket': 'landbrugsdata-processed-data',
                'region': 'europe-west1',
                'temp_location': 'gs://landbrugsdata-processing/temp',
                'requirements_file': 'gs://landbrugsdata-processing/dataflow/requirements.txt'
            }
        )
        validation_tasks.append(validate_task)
    
    # Here we can add more processing steps later
    # For example: analysis, exports, etc. 