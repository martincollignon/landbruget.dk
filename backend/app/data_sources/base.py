from abc import ABC, abstractmethod
import geopandas as gpd
from typing import Dict, Any

class DataSource(ABC):
    """Base class for all data sources."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    @abstractmethod
    def fetch(self) -> gpd.GeoDataFrame:
        """Fetch data and return as GeoDataFrame."""
        pass
    
    @property
    @abstractmethod
    def crs(self) -> str:
        """Return the CRS of the data source."""
        pass
