from abc import ABC, abstractmethod
from typing import Dict, Any
from datetime import datetime
from pydantic import BaseModel

class DataSourceError(Exception):
    """Base exception for data source errors"""
    pass

class SourceData(BaseModel):
    """Data structure that all sources must return"""
    data: Dict[str, Any]
    timestamp: datetime = datetime.now()
    metadata: Dict[str, Any] = {}

class DataSource(ABC):
    """Base class for all data sources"""
    
    @abstractmethod
    def __init__(self, config: dict):
        """Initialize with configuration"""
        pass
    
    @abstractmethod
    async def fetch_data(self) -> SourceData:
        """Fetch data from source"""
        pass

    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """Get source metadata"""
        pass
    
    @abstractmethod
    async def close(self):
        """Cleanup resources"""
        pass
