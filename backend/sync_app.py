import os
import asyncio
import logging
from src.sources.parsers import get_source_handler
from src.config import SOURCES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_sync() -> bool:
    """Run the sync process based on environment variable"""
    sync_type = os.getenv('SYNC_TYPE', 'all')
    logger.info(f"Starting sync process for: {sync_type}")
    
    try:
        if sync_type == 'all':
            for source_id, config in SOURCES.items():
                if config['enabled']:
                    source = get_source_handler(source_id, config)
                    if source:
                        total_synced = await source.sync()
                        logger.info(f"{source_id} sync completed. Total records: {total_synced:,}")
        else:
            if sync_type not in SOURCES:
                logger.error(f"Unknown sync type: {sync_type}")
                return False
                
            config = SOURCES[sync_type]
            if not config['enabled']:
                logger.error(f"Sync type {sync_type} is disabled")
                return False
                
            source = get_source_handler(sync_type, config)
            if not source:
                logger.error(f"No handler for sync type: {sync_type}")
                return False
                
            total_synced = await source.sync()
            logger.info(f"{sync_type} sync completed. Total records: {total_synced:,}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during sync: {str(e)}")
        return False

if __name__ == "__main__":
    asyncio.run(run_sync())
