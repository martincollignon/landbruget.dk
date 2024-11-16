from abc import ABC, abstractmethod
import geopandas as gpd

class Transformer(ABC):
    """Base class for data transformations."""
    
    @abstractmethod
    def transform(self, data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Transform the data."""
        pass

    def __str__(self) -> str:
        """String representation of the transformer."""
        return self.__class__.__name__
