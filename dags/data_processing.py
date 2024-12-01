from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'project_id': 'landbrugsdata-1',
    'region': 'europe-west1'
}

dataflow_options = {
    'project': default_args['project_id'],
    'input_bucket': 'landbrugsdata-raw-data',
    'output_bucket': 'landbrugsdata-processed-data',
    'runner': 'DataflowRunner',
    'max_num_workers': 4,
    'machine_type': 'n1-standard-4',
    'disk_size_gb': 100,
    'region': 'europe-west1',
    'temp_location': 'gs://landbrugsdata-processing/temp',
    'sdk_container_image': 'gcr.io/landbrugsdata-1/dataflow-processing:latest'
}

with DAG(
    'data_processing',
    default_args=default_args,
    description='Data processing workflows for Landbrugsdata',
    schedule_interval=None,
    catchup=False,
    tags=['landbrugsdata', 'processing']
) as dag:
    
    validate_cadastral = DataflowCreatePythonJobOperator(
        task_id='validate_cadastral',
        py_file='gs://landbrugsdata-processing/dataflow/validate_cadastral.py',
        job_name=f'validate-cadastral-{{{{ds_nodash}}}}',
        options={
            **dataflow_options,
            'dataset': 'cadastral'
        },
        location='europe-west1',
        wait_until_finished=True,
        gcp_conn_id='google_cloud_default'
    )

    validate_wetlands = DataflowCreatePythonJobOperator(
        task_id='validate_wetlands',
        py_file='gs://landbrugsdata-processing/dataflow/validate_wetlands.py',
        job_name=f'validate-wetlands-{{{{ds_nodash}}}}',
        options={
            **dataflow_options,
            'dataset': 'wetlands'
        },
        location='europe-west1',
        wait_until_finished=True,
        gcp_conn_id='google_cloud_default'
    )

    validate_water_projects = DataflowCreatePythonJobOperator(
        task_id='validate_water_projects',
        py_file='gs://landbrugsdata-processing/dataflow/validate_water_projects.py',
        job_name=f'validate-water-projects-{{{{ds_nodash}}}}',
        options={
            **dataflow_options,
            'dataset': 'water_projects'
        },
        location='europe-west1',
        wait_until_finished=True,
        gcp_conn_id='google_cloud_default'
    )

    # All tasks can run in parallel
    [validate_cadastral, validate_wetlands, validate_water_projects]