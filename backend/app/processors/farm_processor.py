from .base import Processor
import geopandas as gpd
import logging

logger = logging.getLogger(__name__)

class FarmProcessor(Processor):
    """Processor for farm location data."""
    
    def process(self, data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Process farm data."""
        logger.info("Processing farm data")
        
        if not self.validate_data(data):
            raise ValueError("Invalid input data")
            
        try:
            # Add any farm-specific processing here
            # For example, you might want to:
            # - Filter by specific criteria
            # - Add calculated fields
            # - Merge with other data
            
            logger.info(f"Processed {len(data)} farm records")
            return data
            
        except Exception as e:
            logger.error(f"Error processing farm data: {e}")
            raise
