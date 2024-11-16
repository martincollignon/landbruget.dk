from .base import DataSource
import geopandas as gpd
import pandas as pd
from google.cloud import storage
import tempfile
import os
import logging

logger = logging.getLogger(__name__)

class ExcelDataSource(DataSource):
    """Data source for Excel files (local or GCP)."""
    
    def __init__(self, config: dict):
        super().__init__(config)
        self.bucket_name = config.get('bucket_name') or os.getenv('GCP_BUCKET_NAME')
        self.file_path = config['file_path']
        
    def fetch(self) -> gpd.GeoDataFrame:
        """Fetch data from Excel file."""
        try:
            if self.bucket_name:
                # Fetch from GCP
                storage_client = storage.Client()
                bucket = storage_client.bucket(self.bucket_name)
                blob = bucket.blob(self.file_path)
                
                with tempfile.NamedTemporaryFile(suffix='.xlsx') as temp_file:
                    blob.download_to_filename(temp_file.name)
                    df = pd.read_excel(temp_file.name)
            else:
                # Fetch from local file
                df = pd.read_excel(self.file_path)
            
            # Convert to GeoDataFrame
            gdf = gpd.GeoDataFrame(
                df,
                geometry=gpd.points_from_xy(df.longitude, df.latitude),
                crs=self.crs
            )
            
            return gdf
            
        except Exception as e:
            logger.error(f"Error fetching Excel data: {e}")
            raise
            
    @property
    def crs(self) -> str:
        return self.config.get('crs', 'EPSG:4326')
