from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': 'landbrugsdata-1',
    'region': 'europe-west1'
}

with DAG(
    'data_processing',
    default_args=default_args,
    description='Data processing workflows for Landbrugsdata',
    schedule_interval=None,
    catchup=False,
    tags=['landbrugsdata', 'processing']
) as dag:
    
    datasets = ['cadastral', 'water_projects', 'wetlands']
    validation_tasks = []
    
    for dataset in datasets:
        validate_task = DataflowCreatePythonJobOperator(
            task_id=f'validate_{dataset}',
            py_file='gs://landbrugsdata-processing/dataflow/validate_geometries.py',
            job_name=f'validate-{dataset}-{{{{ds_nodash}}}}',
            options={
                'dataset': dataset,
                'project': 'landbrugsdata-1',
                'input_bucket': 'landbrugsdata-raw-data',
                'output_bucket': 'landbrugsdata-processed-data',
                'region': 'europe-west1',
                'temp_location': 'gs://landbrugsdata-processing/temp',
                'requirements_file': 'gs://landbrugsdata-processing/dataflow/requirements.txt'
            },
            py_requirements=['apache-beam[gcp]>=2.50.0'],
            location='europe-west1',
            wait_until_finished=True
        )
        validation_tasks.append(validate_task)
    
    # DAG documentation
    dag.doc_md = """
    # Data Processing DAG
    This DAG handles the validation and processing of various datasets:
    * Cadastral data
    * Water projects
    * Wetlands
    
    ## Validation Tasks
    Each dataset goes through geometry validation using Apache Beam/Dataflow.
    """