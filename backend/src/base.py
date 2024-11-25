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

    @abstractmethod
    async def fetch(self):
        """Fetch data from source"""
        pass

    @abstractmethod
    async def sync(self, client) -> Optional[int]:
        """Sync data to database, returns number of records synced or None on failure"""
        pass
