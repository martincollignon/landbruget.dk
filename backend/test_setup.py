import asyncio
import logging
from app.data_sources import WFSDataSource
from app.config.sources import get_source_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_wfs():
    try:
        # Get config
        logger.info("Getting WFS config...")
        config = get_source_config("wfs_fvm_markers")
        
        # Create source
        logger.info("Creating WFS source...")
        source = WFSDataSource(config.dict())
        
        # Fetch data
        logger.info("Fetching data...")
        result = await source.fetch_data()
        
        # Print results
        feature_count = len(result.data.get('features', []))
        logger.info(f"Successfully fetched {feature_count} features")
        logger.info(f"Metadata: {result.metadata}")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise
    finally:
        if 'source' in locals():
            await source.close()

if __name__ == "__main__":
    asyncio.run(test_wfs())
