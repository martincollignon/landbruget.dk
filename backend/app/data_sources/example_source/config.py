from typing import Optional
from pydantic import BaseModel
from app.config.sources import SourceConfig

class ExampleSourceConfig(SourceConfig):
    """Configuration for example data source"""
    # Add your source-specific configuration fields here
    url: str
    api_key: Optional[str] = None
