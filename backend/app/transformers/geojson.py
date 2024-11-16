import geopandas as gpd
from typing import Dict, Any
from .base import Transformer

class GeoJSONToGeoDataFrame(Transformer):
    def transform(self, data: Dict[str, Any]) -> gpd.GeoDataFrame:
        """Transform GeoJSON data into a GeoDataFrame"""
        gdf = gpd.GeoDataFrame.from_features(data['features'])
        
        # Set Danish coordinate system (ETRS89 / UTM zone 32N)
        gdf.set_crs(epsg=25832, inplace=True)
            
        return gdf
