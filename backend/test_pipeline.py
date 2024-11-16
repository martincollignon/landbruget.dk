import asyncio
import logging
from app.data_sources import WFSDataSource
from app.config.sources import get_source_config
from app.pipeline import Pipeline
from app.storage.geoparquet import GeoParquetStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_pipeline():
    try:
        # Get config and create source
        logger.info("Setting up pipeline components...")
        config = get_source_config("wfs_fvm_markers")
        source = WFSDataSource(config.dict())
        
        # Create storage (using local storage for testing)
        storage = GeoParquetStorage(
            output_dir="data",  # Local directory for testing
            use_local=True      # Don't try to use Google Cloud Storage
        )
        
        # Create pipeline
        logger.info("Creating pipeline...")
        pipeline = Pipeline(
            source=source,
            storage=storage
        )
        
        # Execute pipeline
        logger.info("Executing pipeline...")
        result = await pipeline.execute()
        logger.info(f"Pipeline completed. Data stored at: {result}")
        
    except Exception as e:
        logger.error(f"Pipeline test failed: {str(e)}")
        raise
    finally:
        if 'source' in locals():
            await source.close()

if __name__ == "__main__":
    asyncio.run(test_pipeline())
