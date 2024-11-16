import aiohttp
from .base import DataSource, DataSourceError, SourceData
from app.config.sources import WFSConfig

class WFSDataSource(DataSource):
    """WFS data source implementation"""
    
    def __init__(self, config: dict):
        self._validate_config(config)
    
    def _validate_config(self, config: dict) -> bool:
        try:
            self.wfs_config = WFSConfig(**config)
            return True
        except Exception as e:
            raise DataSourceError(f"Invalid WFS configuration: {str(e)}")
    
    async def fetch_data(self) -> SourceData:
        params = {
            'service': 'WFS',
            'version': self.wfs_config.version,
            'request': 'GetFeature',
            'typeName': self.wfs_config.layer,
            'outputFormat': 'application/json',
            **self.wfs_config.additional_params
        }
        
        timeout = aiohttp.ClientTimeout(total=self.wfs_config.timeout)
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.wfs_config.url, params=params) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise DataSourceError(
                            f"WFS request failed with status {response.status}: {error_text}"
                        )
                    
                    geojson_data = await response.json()
                    
                    return SourceData(
                        data=geojson_data,
                        metadata={
                            'feature_count': len(geojson_data.get('features', [])),
                            'layer': self.wfs_config.layer,
                            'url': self.wfs_config.url
                        }
                    )
                    
        except aiohttp.ClientError as e:
            raise DataSourceError(f"WFS request failed: {str(e)}")
        except Exception as e:
            raise DataSourceError(f"Error processing WFS data: {str(e)}")
    
    def get_metadata(self) -> dict:
        return {
            'name': self.wfs_config.name,
            'description': self.wfs_config.description,
            'type': 'WFS',
            'url': self.wfs_config.url,
            'layer': self.wfs_config.layer
        }
    
    async def close(self):
        """Nothing to clean up for WFS"""
        pass
