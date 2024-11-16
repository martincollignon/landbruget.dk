from .base import Processor
import geopandas as gpd

class OptimizeDataTypes(Processor):
    def process(self, data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        # Optimize memory usage
        for col in data.select_dtypes(include=['object']).columns:
            if col != 'geometry':
                data[col] = data[col].astype('category')
        return data
