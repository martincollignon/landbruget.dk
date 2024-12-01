from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowPythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),  # Set to yesterday
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 'landbrugsdata-1',  # Added project_id
    'region': 'europe-west1'  # Added region
}

with DAG(
    'data_processing',
    default_args=default_args,
    description='Data processing workflows for Landbrugsdata',
    schedule_interval=None,  # Manual triggers only
    catchup=False,  # Prevent backfilling
    tags=['landbrugsdata', 'processing']
) as dag:
    
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
            },
            py_interpreter='python3',
            py_requirements=['apache-beam[gcp]>=2.50.0'],
            py_system_site_packages=False,
            location='europe-west1',
            wait_until_finished=True
        )
        validation_tasks.append(validate_task)
    
    # Define task dependencies (if any)
    # For now, tasks run independently
    # You could add dependencies like:
    # validate_cadastral >> validate_water_projects >> validate_wetlands
    
    # Add task documentation
    dag.doc_md = """
    # Data Processing DAG
    This DAG handles the validation and processing of various datasets:
    * Cadastral data
    * Water projects
    * Wetlands
    
    ## Validation Tasks
    Each dataset goes through geometry validation using Apache Beam/Dataflow.
    """