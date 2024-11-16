from enum import Enum
from typing import Dict, Any, Optional
from pydantic import BaseModel

class DataSourceType(Enum):
    """Types of data sources supported by the application."""
    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"
    API = "api"

class DataSourceConfig(BaseModel):
    """Configuration for a data source."""
    type: DataSourceType
    name: str
    description: str
    path: Optional[str] = None  # For file-based sources
    url: Optional[str] = None   # For API-based sources
    credentials: Optional[Dict[str, Any]] = None
    parameters: Optional[Dict[str, Any]] = None

# Example data sources configuration
DATA_SOURCES = {
    "farms": DataSourceConfig(
        type=DataSourceType.EXCEL,
        name="Farm Locations",
        description="Farm locations across Europe",
        path="app/data/farms.xlsx",  # Updated path
    ),
    # Add more data sources here as needed
}