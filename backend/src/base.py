from abc import ABC, abstractmethod
from google.cloud import storage
import geopandas as gpd
from shapely.geometry import shape
import pyarrow as pa
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class Source(ABC):
    def __init__(self, config):
        self.config = config
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket('landbrugsdata-raw-data')
    
    async def write_to_storage(self, features, type_name):
        """Convert features to GeoParquet and write to Cloud Storage"""
        try:
            # Convert features to GeoDataFrame
            gdf = gpd.GeoDataFrame.from_features(features)
            
            # Write to temporary file (Cloud Run has ephemeral storage)
            temp_path = f'/tmp/{type_name}.parquet'
            gdf.to_parquet(temp_path)
            
            # Upload to Cloud Storage
            blob = self.bucket.blob(f'raw/{type_name}/current.parquet')
            blob.upload_from_filename(temp_path)
            
            logger.info(f"Wrote {len(gdf)} features to {blob.name}")
            
        except Exception as e:
            logger.error(f"Error writing to storage: {str(e)}")
            raise

    @abstractmethod
    async def fetch(self):
        """Fetch data from source"""
        pass

    @abstractmethod
    async def sync(self) -> Optional[int]:
        """Sync data to storage, returns number of records synced or None on failure"""
        pass
