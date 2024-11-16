from abc import ABC, abstractmethod
import geopandas as gpd
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class Processor(ABC):
    """Base class for data processors."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
    
    @abstractmethod
    def process(self, data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Process the data."""
        pass

    def __str__(self) -> str:
        """String representation of the processor."""
        return self.__class__.__name__

    def validate_data(self, data: gpd.GeoDataFrame) -> bool:
        """Validate the input data."""
        if not isinstance(data, gpd.GeoDataFrame):
            logger.error("Input must be a GeoDataFrame")
            return False
        return True
