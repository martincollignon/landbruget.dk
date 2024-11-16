from ..base import DataSource, SourceData, DataSourceError
from .config import SourceConfig

class ExampleDataSource(DataSource):
    """Template for implementing new data sources"""
    
    def __init__(self, config: dict):
        self._validate_config(config)
    
    def _validate_config(self, config: dict) -> bool:
        try:
            self.config = SourceConfig(**config)
            return True
        except Exception as e:
            raise DataSourceError(f"Invalid configuration: {str(e)}")
    
    async def fetch_data(self) -> SourceData:
        """
        Implement your data fetching logic here.
        See WFSDataSource for a complete example.
        """
        try:
            # Your implementation here
            data = {}  # Replace with actual data fetching
            
            return SourceData(
                data=data,
                metadata={
                    'source_name': self.config.name,
                    'record_count': 0
                }
            )
            
        except Exception as e:
            raise DataSourceError(f"Failed to fetch data: {str(e)}")
    
    async def close(self):
        """Cleanup any resources"""
        pass
