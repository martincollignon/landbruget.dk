from abc import ABC, abstractmethod
import pandas as pd
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

def clean_value(value):
    """Clean a value by removing whitespace and converting empty strings to None"""
    if isinstance(value, str):
        value = value.strip()
        return None if value == '' else value
    return value

class Source(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.batch_size: int = config.get('batch_size', 1000)
        self.max_concurrent: int = config.get('max_concurrent', 10)

    @abstractmethod
    async def fetch(self) -> pd.DataFrame:
        """Fetch data from source and return as DataFrame"""
        pass

    @abstractmethod
    async def sync(self, client) -> int:
        """Sync data to database, returns number of records synced"""
        pass
