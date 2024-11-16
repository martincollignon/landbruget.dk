import aiohttp
import zipfile
import io
import logging
import subprocess
import json
from pathlib import Path
from ..base import DataSource, SourceData, DataSourceError
from .config import ShapefileConfig

logger = logging.getLogger(__name__)

class ShapefileDataSource(DataSource):
    """Data source for ZIP files containing shapefiles"""
    
    def __init__(self, config: dict):
        self.temp_dir = Path("temp_data")
        self._validate_config(config)
    
    def _validate_config(self, config: dict) -> bool:
        try:
            self.config = ShapefileConfig(**config)
            return True
        except Exception as e:
            raise DataSourceError(f"Invalid shapefile configuration: {str(e)}")
    
    def get_metadata(self) -> dict:
        """Return metadata about this data source"""
        return {
            "name": self.config.name,
            "description": self.config.description,
            "type": "shapefile",
            "url": self.config.url,
            "update_frequency": "monthly"
        }
    
    async def fetch_data(self) -> SourceData:
        try:
            self.temp_dir.mkdir(exist_ok=True)
            
            # Download ZIP file
            async with aiohttp.ClientSession() as session:
                async with session.get(self.config.url) as response:
                    if response.status != 200:
                        raise DataSourceError(f"Failed to download: {response.status}")
                    content = await response.read()
            
            # Extract files
            with zipfile.ZipFile(io.BytesIO(content)) as zip_ref:
                zip_ref.extractall(self.temp_dir)
            
            # Convert to GeoJSON
            shp_path = self.temp_dir / f"{self.config.filename}.shp"
            geojson_path = self.temp_dir / f"{self.config.filename}.geojson"
            
            result = subprocess.run([
                'ogr2ogr',
                '-f', 'GeoJSON',
                str(geojson_path),
                str(shp_path)
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                raise DataSourceError(f"Conversion failed: {result.stderr}")
            
            # Read GeoJSON
            with open(geojson_path) as f:
                data = json.load(f)
            
            return SourceData(
                data=data,
                metadata={
                    'source_name': self.config.name,
                    'feature_count': len(data['features']),
                    'source_type': 'shapefile'
                }
            )
            
        except Exception as e:
            raise DataSourceError(f"Failed to process shapefile: {str(e)}")
        finally:
            await self.close()
    
    async def close(self):
        """Cleanup temporary files"""
        if self.temp_dir.exists():
            import shutil
            shutil.rmtree(self.temp_dir)