from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd
from datetime import datetime

class Source(ABC):
    """Base class for all data sources"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = config['name']
        self.frequency = config.get('frequency', 'static')
        
    @abstractmethod
    async def fetch(self) -> pd.DataFrame:
        """Fetch and transform data into final DataFrame format"""
        pass
    
    @property
    def metadata(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'frequency': self.frequency,
            'last_updated': datetime.now().isoformat()
        }
