import pandas as pd
import geopandas as gpd
import aiohttp
from ..base import Source

class AgriculturalFields(Source):
    """Danish Agricultural Fields WFS parser"""
    
    async def fetch(self) -> pd.DataFrame:
        async with aiohttp.ClientSession() as session:
            params = {
                'service': 'WFS',
                'version': '2.0.0',
                'request': 'GetFeature',
                'typeName': self.config['layer'],
                'outputFormat': 'application/json'
            }
            
            async with session.get(self.config['url'], params=params) as response:
                response.raise_for_status()
                data = await response.json()
                
            # Convert to GeoDataFrame and standardize columns
            gdf = gpd.GeoDataFrame.from_features(data['features'])
            gdf = gdf.rename(columns={
                'marknr': 'field_id',
                'afgroede': 'crop_type',
                'areal': 'area_ha'
            })
            
            return gdf[['field_id', 'crop_type', 'area_ha', 'geometry']]
