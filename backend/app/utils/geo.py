import geopandas as gpd
from typing import Dict, Any

def geojson_to_gdf(geojson_data: Dict[str, Any]) -> gpd.GeoDataFrame:
    """Convert GeoJSON to GeoDataFrame"""
    return gpd.GeoDataFrame.from_features(geojson_data['features'])
