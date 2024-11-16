from typing import List, Dict, Any
from app.data_sources.base import DataSource
from app.transformers.base import Transformer
from app.processors.base import Processor
from app.storage.base import Storage
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class Pipeline:
    def __init__(
        self,
        source: DataSource,
        storage: Storage,
        transformers: List[Transformer] = None,
        processors: List[Processor] = None
    ):
        self.source = source
        self.storage = storage
        self.transformers = transformers or []
        self.processors = processors or []
    
    async def execute(self) -> str:
        """Execute the complete pipeline"""
        try:
            # 1. Fetch data
            logger.info("Fetching data from source")
            result = await self.source.fetch_data()
            
            # 2. Store raw data
            logger.info("Storing raw data")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            raw_path = self.storage.store_raw(result.data, f"data_{timestamp}")
            
            # 3. Apply transformations
            data = result.data
            for transformer in self.transformers:
                logger.info(f"Applying transformer: {transformer.__class__.__name__}")
                data = transformer.transform(data)
            
            # 4. Apply processors
            for processor in self.processors:
                logger.info(f"Applying processor: {processor.__class__.__name__}")
                data = processor.process(data)
            
            # 5. Store processed data
            logger.info("Storing processed data")
            final_path = self.storage.store(data, f"data_{timestamp}")
            
            return final_path
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise