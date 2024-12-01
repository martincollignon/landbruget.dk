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

with DAG(
    'data_processing',
    default_args=default_args,
    description='Data processing workflows for Landbrugsdata',
    schedule_interval=None,
    catchup=False,
    tags=['landbrugsdata', 'processing']
) as dag:
    
    validate_task = DataflowCreatePythonJobOperator(
        task_id='validate_test',
        py_file='gs://landbrugsdata-processing/dataflow/validate_geometries.py',
        job_name=f'validate-test-{{{{ds_nodash}}}}',
        options={
            'project': default_args['project_id'],
            'dataset': 'cadastral',
            'input_bucket': 'landbrugsdata-raw-data',
            'output_bucket': 'landbrugsdata-processed-data',
            'runner': 'DataflowRunner',
            'max_num_workers': 4,
            'machine_type': 'n1-standard-4',
            'disk_size_gb': 100,
            'region': 'europe-west1',
            'temp_location': 'gs://landbrugsdata-processing/temp',
            'setup_file': None,
            'requirements_file': None,
            'python_setup_file': None,
            'sdk_container_image': 'gcr.io/landbrugsdata-1/dataflow-processing:latest'
        },
        location='europe-west1',
        wait_until_finished=True,
        gcp_conn_id='google_cloud_default'
    )