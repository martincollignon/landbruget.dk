from abc import ABC, abstractmethod
from google.cloud import storage
import geopandas as gpd
from shapely.geometry import shape
import pyarrow as pa
import logging
from typing import Optional
import time
import os
import pandas as pd
from .sources.utils.geometry_validator import validate_and_transform_geometries

logger = logging.getLogger(__name__)

class Source(ABC):
    def __init__(self, config):
        self.config = config
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket('landbrugsdata-raw-data')
    
    async def write_to_storage(self, features, dataset):
        """Write features to GeoParquet in Cloud Storage, safely appending to temp file"""
        if not features:
            return
            
        try:
            # Create new GeoDataFrame from features
            new_gdf = gpd.GeoDataFrame(features, crs="EPSG:25832")
            
            # Clean up column names
            new_gdf.columns = [col.replace('.', '_').replace('(', '_').replace(')', '_') for col in new_gdf.columns]
            
            # Validate and transform geometries
            new_gdf = validate_and_transform_geometries(new_gdf, dataset)
            
            # Use temporary files for both working and final versions
            temp_working = f"/tmp/{dataset}_working_{int(time.time())}.parquet"
            temp_final = f"/tmp/{dataset}_final_{int(time.time())}.parquet"
            
            # Check if existing temp working file exists
            working_blob = self.bucket.blob(f'raw/{dataset}/working.parquet')
            
            if working_blob.exists():
                # Download existing working file
                working_blob.download_to_filename(temp_working)
                existing_gdf = gpd.read_parquet(temp_working)
                
                # Append new data
                combined_gdf = pd.concat([existing_gdf, new_gdf], ignore_index=True)
            else:
                combined_gdf = new_gdf
            
            # Write to working file
            combined_gdf.to_parquet(temp_working)
            working_blob.upload_from_filename(temp_working)
            
            # When sync is complete, copy working to final
            if self.is_sync_complete:  # We'll need to add this flag
                combined_gdf.to_parquet(temp_final)
                final_blob = self.bucket.blob(f'raw/{dataset}/current.parquet')
                final_blob.upload_from_filename(temp_final)
                working_blob.delete()  # Clean up working file
                os.remove(temp_final)
            
            # Cleanup working file
            os.remove(temp_working)
            
            logger.info(f"Successfully wrote {len(new_gdf)} features (total: {len(combined_gdf)})")
            
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
