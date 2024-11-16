from typing import List, Optional
from app.data_sources.base import DataSource
from app.transformers.base import Transformer
from app.processors.base import Processor
from app.storage.base import Storage
import logging

logger = logging.getLogger(__name__)

class Pipeline:
    """Data processing pipeline"""
    
    def __init__(
        self,
        source: DataSource,
        storage: Storage,
        transformers: Optional[List[Transformer]] = None,
        processors: Optional[List[Processor]] = None
    ):
        self.source = source
        self.storage = storage
        self.transformers = transformers or []
        self.processors = processors or []
    
    async def execute(self) -> str:
        """Execute pipeline and return storage path"""
        try:
            # 1. Fetch
            logger.info(f"Fetching data from {self.source.__class__.__name__}")
            result = await self.source.fetch_data()
            
            # 2. Transform
            data = result.data
            for transformer in self.transformers:
                logger.info(f"Applying {transformer.__class__.__name__}")
                data = transformer.transform(data)
            
            # 3. Process
            for processor in self.processors:
                logger.info(f"Applying {processor.__class__.__name__}")
                data = processor.process(data)
            
            # 4. Store
            return await self.storage.store(data)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            await self.source.close()
