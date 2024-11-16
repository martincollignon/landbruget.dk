from abc import ABC, abstractmethod
import geopandas as gpd
from typing import Dict, Any, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class Storage(ABC):
    """Base class for data storage."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.timestamp = datetime.now()

    @abstractmethod
    def store(self, data: gpd.GeoDataFrame, name: str) -> str:
        """
        Store the data.
        
        Args:
            data: The GeoDataFrame to store
            name: Name/identifier for the dataset
            
        Returns:
            str: Path or identifier where the data was stored
        """
        pass
    
    @abstractmethod
    def retrieve(self, name: str, version: Optional[str] = None) -> gpd.GeoDataFrame:
        """
        Retrieve the data.
        
        Args:
            name: Name/identifier of the dataset
            version: Optional version/timestamp of the data
            
        Returns:
            GeoDataFrame: The retrieved data
        """
        pass

    @abstractmethod
    def list_versions(self, name: str) -> list:
        """List available versions of a dataset."""
        pass

    def generate_path(self, name: str, extension: str) -> str:
        """Generate a path for storing data."""
        timestamp = self.timestamp.strftime('%Y%m%d_%H%M%S')
        return f"{name}/{timestamp}.{extension}"
