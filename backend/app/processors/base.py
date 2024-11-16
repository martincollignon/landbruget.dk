from abc import ABC, abstractmethod
from typing import Dict, Any

class GeoDataProcessor(ABC):
    """Base class for all geo data processors."""
    
    @abstractmethod
    def process(self) -> dict:
        """Process the data source and return GeoJSON."""
        pass

    def create_feature(self, properties: Dict[str, Any], coordinates: tuple) -> dict:
        """Create a GeoJSON Feature object."""
        return {
            "type": "Feature",
            "properties": properties,
            "geometry": {
                "type": "Point",
                "coordinates": coordinates
            }
        }