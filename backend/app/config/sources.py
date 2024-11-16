from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
from enum import Enum

class DataSourceType(str, Enum):
    """Supported data source types"""
    WFS = "wfs"
    EXCEL = "excel"

class SourceConfig(BaseModel):
    """Base configuration for all data sources"""
    type: DataSourceType
    name: str
    description: Optional[str] = None
    enabled: bool = True
    timeout: int = 600  # 10 minutes default timeout

class WFSConfig(SourceConfig):
    """WFS specific configuration"""
    type: DataSourceType = DataSourceType.WFS
    url: str
    layer: str
    version: str = "2.0.0"
    additional_params: Dict[str, Any] = Field(default_factory=dict)

# Active data sources configuration
SOURCES = {
    "wfs_fvm_markers": WFSConfig(
        name="Danish Agricultural Markers",
        description="Latest marker data from Danish agricultural database",
        url="https://geodata.fvm.dk/geoserver/wfs",
        layer="Marker:Marker_seneste"
    )
}

def get_source_config(source_id: str) -> SourceConfig:
    """Get configuration for a specific source"""
    if source_id not in SOURCES:
        raise KeyError(f"Unknown source: {source_id}")
    return SOURCES[source_id]