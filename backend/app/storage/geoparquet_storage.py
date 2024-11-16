from pathlib import Path
import geopandas as gpd
from google.cloud import storage
from datetime import datetime
import json
import tempfile
import logging
from typing import Optional, Dict, Any
from .base import Storage

logger = logging.getLogger(__name__)

class GeoParquetStorage(Storage):
    """Storage implementation using GeoParquet format"""
    
    def __init__(self, bucket_name: str, project_id: str):
        super().__init__({'bucket': bucket_name, 'project': project_id})
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)
        
        # Create local directories
        self.data_dir = Path('data')
        self.raw_dir = self.data_dir / 'raw'
        self.processed_dir = self.data_dir / 'processed'
        
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def store_raw(self, data: Dict[str, Any], name: str) -> str:
        """Store raw data as JSON"""
        try:
            # Generate paths
            filename = f"{name}.geojson"
            local_path = self.raw_dir / filename
            gcs_path = f"raw/{filename}"
            
            # Store locally
            logger.info(f"Storing raw data at: {local_path}")
            with open(local_path, 'w') as f:
                json.dump(data, f)
                
            # Try to upload to GCS
            try:
                blob = self.bucket.blob(gcs_path)
                blob.upload_from_filename(str(local_path))
                return f"gs://{self.bucket.name}/{gcs_path}"
            except Exception as e:
                logger.warning(f"GCS upload failed: {str(e)}")
                return str(local_path)
            
        except Exception as e:
            logger.error(f"Failed to store raw data: {str(e)}")
            raise
    
    def store(self, data: gpd.GeoDataFrame, name: str) -> str:
        """Store GeoDataFrame as GeoParquet"""
        # Generate path
        path = self.generate_path(name, "parquet")
        
        try:
            # Ensure EPSG:4326
            if data.crs != "EPSG:4326":
                data = data.to_crs("EPSG:4326")
            
            # Store locally first
            local_path = self.processed_dir / path.split('/')[-1]
            data.to_parquet(local_path, index=False)
            
            # Try to upload to GCS
            try:
                blob = self.bucket.blob(path)
                blob.upload_from_filename(str(local_path))
                return f"gs://{self.bucket.name}/{path}"
            except Exception as e:
                logger.warning(f"GCS upload failed: {str(e)}")
                return str(local_path)
            
        except Exception as e:
            logger.error(f"Failed to store data: {str(e)}")
            raise
    
    def retrieve(self, name: str, version: Optional[str] = None) -> gpd.GeoDataFrame:
        """Retrieve GeoParquet as GeoDataFrame"""
        if version:
            path = f"{name}/{version}.parquet"
        else:
            # Get latest version
            blobs = list(self.bucket.list_blobs(prefix=f"{name}/"))
            if not blobs:
                raise ValueError(f"No data found for {name}")
            path = max(blobs, key=lambda x: x.name).name
        
        with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
            self.bucket.blob(path).download_to_filename(tmp.name)
            return gpd.read_parquet(tmp.name)
    
    def list_versions(self, name: str) -> list:
        """List available versions of a dataset"""
        versions = []
        for blob in self.bucket.list_blobs(prefix=f"{name}/"):
            if blob.name.endswith('.parquet'):
                version = blob.name.split('/')[-1].replace('.parquet', '')
                versions.append(version)
        return sorted(versions, reverse=True)