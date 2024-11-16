from typing import Dict, Any, Optional
from pydantic import BaseModel
from app.config.sources import SourceConfig, DataSourceType

class WFSConfig(SourceConfig):
    """WFS specific configuration"""
    type: DataSourceType = DataSourceType.WFS
    url: str
    layer: str
    version: str = "2.0.0"
    additional_params: Dict[str, Any] = {} 