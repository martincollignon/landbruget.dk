import logging
import json
import zipfile
import ijson
from typing import Generator, Any, Dict, Optional, Tuple
from ...base import Source
from google.cloud import storage
from pathlib import Path
import os
import pandas as pd
import asyncio
from datetime import datetime
import tempfile
import paramiko
from google.cloud import secretmanager
from google.cloud import bigquery

logger = logging.getLogger(__name__)

class PropertyOwnersParser(Source):
    """Parser for property owners data from Datafordeler"""
    
    def __init__(self, config):
        super().__init__(config)
        self.batch_size = 1000
        self.bucket_name = config.get('bucket', 'landbrugsdata-raw-data')
        self.raw_folder = "raw"
        self._get_sftp_credentials()
        self.bq_client = bigquery.Client()
        self.dataset_id = 'land_data'
        self.table_id = 'property_owners_raw'
        
    def _get_sftp_credentials(self):
        """Get SFTP credentials from Secret Manager."""
        try:
            secret_client = secretmanager.SecretManagerServiceClient()
            
            # Get host
            host_secret = "projects/677523729299/secrets/datafordeler-sftp-host/versions/latest"
            host_response = secret_client.access_secret_version(name=host_secret)
            self.sftp_host = host_response.payload.data.decode('UTF-8')
            
            # Get username
            username_secret = "projects/677523729299/secrets/datafordeler-sftp-username/versions/latest"
            username_response = secret_client.access_secret_version(name=username_secret)
            self.sftp_username = username_response.payload.data.decode('UTF-8')
            
        except Exception as e:
            logger.error(f"Failed to get SFTP credentials from Secret Manager: {str(e)}")
            raise
        
    def _get_ssh_key(self) -> str:
        """Get SSH private key from Secret Manager."""
        try:
            secret_client = secretmanager.SecretManagerServiceClient()
            secret_name = "projects/677523729299/secrets/ssh-brugerdatafordeleren/versions/3"
            
            response = secret_client.access_secret_version(name=secret_name)
            return response.payload.data.decode('UTF-8')
            
        except Exception as e:
            logger.error(f"Failed to get SSH key from Secret Manager: {str(e)}")
            raise
        
    def _get_sftp_client(self) -> paramiko.SFTPClient:
        """Create and return an SFTP client connection."""
        try:
            # Initialize SSH client
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Get private key from Secret Manager
            private_key_data = self._get_ssh_key()
            
            # Create a temporary file for the key
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as key_file:
                key_file.write(private_key_data)
                key_file.flush()
                
                try:
                    # Load private key
                    private_key = paramiko.RSAKey.from_private_key_file(key_file.name)
                    
                    # Connect to SFTP server
                    ssh.connect(
                        hostname=self.sftp_host,
                        port=22,
                        username=self.sftp_username,
                        pkey=private_key,
                    )
                    
                    return ssh.open_sftp()
                    
                finally:
                    # Clean up the temporary key file
                    os.unlink(key_file.name)
                    
        except Exception as e:
            logger.error(f"Failed to connect to SFTP server: {str(e)}")
            raise

    def _find_latest_file(self, sftp: paramiko.SFTPClient) -> Tuple[str, datetime]:
        """
        Find the latest .zip file in the SFTP server.
        
        Returns:
            Tuple of (filename, modification_time)
        """
        latest_file = None
        latest_time = None
        
        try:
            # List all files in the root directory
            for entry in sftp.listdir_attr():
                # Check if it's a file and ends with .zip
                if not entry.longname.startswith('d') and entry.filename.endswith('.zip'):
                    mod_time = datetime.fromtimestamp(entry.st_mtime)
                    if latest_time is None or mod_time > latest_time:
                        latest_file = entry.filename
                        latest_time = mod_time
            
            if latest_file is None:
                raise FileNotFoundError("No .zip files found on SFTP server")
                
            logger.info(f"Found latest file: {latest_file} (modified: {latest_time})")
            return latest_file, latest_time
            
        except Exception as e:
            logger.error(f"Error listing SFTP directory: {str(e)}")
            raise
    
    def _get_gcs_blob(self, filename: str) -> storage.Blob:
        """Get a GCS blob for the given filename."""
        bucket = self.storage_client.bucket(self.bucket_name)
        blob_name = f"{self.raw_folder}/{filename}"
        return bucket.blob(blob_name)
    
    def build_nested_dict(self, d: dict, keys: list, value: Any) -> dict:
        """
        Helper function to build nested dictionary structure.
        
        Args:
            d: Dictionary to build into
            keys: List of keys representing the path
            value: Value to set at the final key
        """
        current = d
        for key in keys[:-1]:
            current = current.setdefault(key, {})
        current[keys[-1]] = value
        return d

    def stream_and_transform(self, input_path: str, output_path: str) -> int:
        """
        Stream the zipped JSON file and transform it to newline-delimited JSON.
        Maintains nested structure of the original data.
        """
        logger.info(f"Starting transformation of {input_path}")
        
        tmp_output = f"{output_path}.tmp"
        records_processed = 0
        
        try:
            with zipfile.ZipFile(input_path, 'r') as zip_file:
                json_files = [f for f in zip_file.namelist() if f.endswith('.json')]
                if not json_files:
                    raise ValueError("No JSON file found in ZIP archive")
                
                json_file = json_files[0]
                logger.info(f"Processing JSON file from ZIP: {json_file}")
                
                with zip_file.open(json_file) as json_data, open(tmp_output, 'w') as out_file:
                    parser = ijson.parse(json_data)
                    current_object = {"type": None, "properties": {}}
                    current_path = []
                    in_properties = False
                    
                    for prefix, event, value in parser:
                        if prefix.startswith('features.item'):
                            parts = prefix.split('.')
                            
                            if len(parts) == 2 and event == 'start_map':
                                current_object = {"type": None, "properties": {}}
                                current_path = []
                                in_properties = False
                            elif len(parts) == 2 and event == 'end_map':
                                if current_object:  # Only write non-empty objects
                                    json.dump(current_object, out_file)
                                    out_file.write('\n')
                                    records_processed += 1
                            elif parts[-1] == 'type' and len(parts) == 3:
                                current_object['type'] = value
                            elif parts[-1] == 'properties' and event == 'start_map':
                                in_properties = True
                            elif parts[-1] == 'properties' and event == 'end_map':
                                in_properties = False
                            elif in_properties and event in ('string', 'number', 'boolean', 'null'):
                                # Get the path after 'properties'
                                property_path = parts[parts.index('properties') + 1:]
                                # Build nested structure under properties
                                self.build_nested_dict(current_object['properties'], property_path, value)
            
            os.rename(tmp_output, output_path)
            logger.info(f"Successfully transformed {input_path} to {output_path} ({records_processed} records)")
            return records_processed
            
        except Exception as e:
            if os.path.exists(tmp_output):
                os.remove(tmp_output)
            logger.error(f"Error transforming file: {str(e)}")
            raise
    
    def upload_to_gcs(self, local_path: str, gcs_filename: str) -> str:
        """
        Upload a file to Google Cloud Storage.
        
        Args:
            local_path: Path to the local file
            gcs_filename: Desired filename in GCS
            
        Returns:
            GCS URI of the uploaded file
        """
        blob = self._get_gcs_blob(gcs_filename)
        blob.upload_from_filename(local_path)
        
        gcs_uri = f"gs://{self.bucket_name}/{self.raw_folder}/{gcs_filename}"
        logger.info(f"File uploaded to {gcs_uri}")
        return gcs_uri
    
    async def fetch(self) -> pd.DataFrame:
        """
        Fetch the latest data from GCS and return as DataFrame.
        Required by Source base class.
        """
        # List blobs in the raw folder
        blobs = list(self.storage_client.list_blobs(
            self.bucket_name,
            prefix=f"{self.raw_folder}/",
            delimiter='/'
        ))
        
        if not blobs:
            raise ValueError("No data found in GCS bucket")
        
        # Get the latest blob (by name or creation time)
        latest_blob = max(blobs, key=lambda b: b.time_created)
        
        # Create a temporary file to store the data
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.ndjson') as temp_file:
            latest_blob.download_to_filename(temp_file.name)
            df = pd.read_json(temp_file.name, lines=True)
            return df
    
    async def sync(self) -> Optional[int]:
        """Sync data from SFTP to GCS and BigQuery"""
        sftp_client = None
        temp_input = None
        temp_output = None
        records_processed = None
        
        try:
            # Connect to SFTP and download latest file
            sftp_client = self._get_sftp_client()
            latest_file, mod_time = self._find_latest_file(sftp_client)
            
            # Create temporary files
            temp_input = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
            temp_output = tempfile.NamedTemporaryFile(delete=False, suffix='.ndjson')
            temp_input.close()
            temp_output.close()
            
            # Download and transform
            logger.info(f"Downloading {latest_file}...")
            sftp_client.get(latest_file, temp_input.name)
            records_processed = self.stream_and_transform(temp_input.name, temp_output.name)
            
            # Upload to GCS
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            original_name = Path(latest_file).stem
            gcs_filename = f"property_owners_{original_name}_{timestamp}.ndjson"
            gcs_uri = self.upload_to_gcs(temp_output.name, gcs_filename)
            
            # Load to BigQuery from GCS
            logger.info("Loading data to BigQuery...")
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True  # Let BigQuery detect the schema
            )
            
            table_ref = f"{self.dataset_id}.{self.table_id}"
            load_job = self.bq_client.load_table_from_uri(
                gcs_uri,
                table_ref,
                job_config=job_config
            )
            load_job.result()  # Wait for job to complete
            
            logger.info(f"Loaded {records_processed} records to BigQuery table {table_ref}")
            return records_processed
            
        finally:
            # Clean up
            if sftp_client:
                sftp_client.close()
            
            for temp_file in [temp_input, temp_output]:
                if temp_file and os.path.exists(temp_file.name):
                    os.remove(temp_file.name)
    
    def process_file(self, input_path: str, base_filename: str) -> str:
        """
        Process the input file and upload to GCS.
        
        Args:
            input_path: Path to the input ZIP file
            base_filename: Base name for the output file
            
        Returns:
            GCS URI of the processed file
        """
        # Create output filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f"{Path(base_filename).stem}_{timestamp}.ndjson"
        output_path = f"/tmp/{output_filename}"
        
        try:
            # Transform the file to newline-delimited JSON
            self.stream_and_transform(input_path, output_path)
            
            # Upload to GCS
            gcs_uri = self.upload_to_gcs(output_path, output_filename)
            
            # Clean up local files
            os.remove(output_path)
            
            return gcs_uri
            
        except Exception as e:
            logger.error(f"Failed to process file: {str(e)}")
            # Clean up any remaining temporary files
            if os.path.exists(output_path):
                os.remove(output_path)
            raise