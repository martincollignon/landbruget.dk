from typing import Optional
from app.config.sources import SourceConfig, DataSourceType

class ShapefileConfig(SourceConfig):
    """Configuration for Shapefile data sources"""
    type: DataSourceType = DataSourceType.SHAPEFILE
    url: str
    filename: str  # Base filename within the ZIP (without extension)