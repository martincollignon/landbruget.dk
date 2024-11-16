import asyncio
import logging
import os
from app.pipeline import Pipeline
from app.data_sources import WFSDataSource
from app.storage import GeoParquetStorage
from app.transformers.geojson import GeoJSONToGeoDataFrame
from app.processors.optimize import OptimizeDataTypes
from app.config.sources import get_source_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    try:
        # Get configurations
        config = get_source_config("wfs_fvm_markers")
        
        # Initialize components
        source = WFSDataSource(config.dict())
        storage = GeoParquetStorage(
            bucket_name=os.getenv('GCS_BUCKET', 'landbrugsdata-geodata'),
            project_id=os.getenv('GOOGLE_CLOUD_PROJECT', 'landbrugsdata-1')
        )
        
        # Create pipeline
        pipeline = Pipeline(
            source=source,
            storage=storage,
            transformers=[GeoJSONToGeoDataFrame()],
            processors=[OptimizeDataTypes()]
        )
        
        # Execute pipeline
        result_path = await pipeline.execute()
        logger.info(f"Pipeline completed. Data stored at: {result_path}")
        
    except Exception as e:
        logger.error(f"Sync failed: {str(e)}")
        raise
    finally:
        if 'source' in locals():
            await source.close()

if __name__ == "__main__":
    asyncio.run(main())
