from abc import ABC, abstractmethod
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def clean_value(value):
    """Clean a value by removing whitespace and converting empty strings to None"""
    if isinstance(value, str):
        value = value.strip()
        return None if value == '' else value
    return value

class Source(ABC):
    def __init__(self, config):
        self.config = config

    @abstractmethod
    async def fetch(self) -> pd.DataFrame:
        """Fetch data from source and return as DataFrame"""
        pass
