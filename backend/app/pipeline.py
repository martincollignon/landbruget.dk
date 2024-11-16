from typing import List, Dict, Any
from app.data_sources.base import DataSource
from app.transformers.base import Transformer
from app.processors.base import Processor
from app.storage.base import Storage
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class Pipeline:
    """Orchestrates the data processing pipeline."""
    
    def __init__(
        self,
        name: str,
        source: DataSource,
        transformers: List[Transformer],
        processors: List[Processor],
        storage: Storage,
        config: Dict[str, Any] = None
    ):
        self.name = name
        self.source = source
        self.transformers = transformers
        self.processors = processors
        self.storage = storage
        self.config = config or {}
        self.timestamp = datetime.now()
    
    def run(self) -> bool:
        """Execute the pipeline."""
        try:
            # 1. Fetch data
            logger.info(f"[{self.name}] Fetching data from source")
            data = self.source.fetch()
            
            # Store raw data
            logger.info(f"[{self.name}] Storing raw data")
            raw_path = self.storage.store(data, f"raw/{self.name}")
            logger.info(f"[{self.name}] Raw data stored at: {raw_path}")
            
            # 2. Apply transformations
            for transformer in self.transformers:
                logger.info(f"[{self.name}] Applying transformer: {transformer}")
                data = transformer.transform(data)
            
            # 3. Apply processors
            for processor in self.processors:
                logger.info(f"[{self.name}] Applying processor: {processor}")
                data = processor.process(data)
            
            # 4. Store processed results
            logger.info(f"[{self.name}] Storing processed data")
            processed_path = self.storage.store(data, f"processed/{self.name}")
            logger.info(f"[{self.name}] Processed data stored at: {processed_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"[{self.name}] Pipeline error: {e}")
            raise

    def __str__(self) -> str:
        return f"Pipeline({self.name})"
