from ..base import DataSource, SourceData
from .config import WFSConfig
import aiohttp

class DanishWFSSource(DataSource):
    """Danish Web Feature Service data source"""
    
    def __init__(self, config: WFSConfig):
        self.config = config
    
    async def fetch_data(self) -> SourceData:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.config.url, params={
                'service': 'WFS',
                'version': '2.0.0',
                'request': 'GetFeature',
                'typeName': self.config.layer,
                'outputFormat': 'application/json'
            }) as response:
                data = await response.json()
                
                return SourceData(
                    data=data,
                    metadata=self.get_metadata()
                )
    
    def get_metadata(self):
        return {
            "name": "Danish Agricultural Fields",
            "description": "Agricultural field markers from Danish database",
            "update_frequency": "weekly",
            "fields": {
                "markerID": "Unique identifier for marker",
                "cropType": "Type of crop in field",
                "area": "Field area in hectares"
            }
        } 