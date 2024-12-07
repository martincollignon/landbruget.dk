from abc import ABC, abstractmethod
from google.cloud import storage
import geopandas as gpd
from shapely.geometry import shape
import pyarrow as pa
import logging
from typing import Optional
import time
import os
import pandas as pd
from .sources.utils.geometry_validator import validate_and_transform_geometries

logger = logging.getLogger(__name__)

class Source(ABC):
    """Abstract base class for data sources"""
    
    def __init__(self, config):
        self.config = config
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket('landbrugsdata-raw-data')
    
    @abstractmethod
    async def fetch(self):
        """Fetch data from source"""
        pass

    @abstractmethod
    async def sync(self) -> Optional[int]:
        """Sync data to storage, returns number of records synced or None on failure"""
        pass
