import asyncio
import logging
import os
from scripts.sync_cadastral import main as sync_cadastral
from scripts.sync_wetlands import main as sync_wetlands

# Configure logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_sync() -> bool:
    """Run the sync process based on environment variable"""
    sync_type = os.getenv('SYNC_TYPE', 'all')
    logger.info(f"Starting sync process for: {sync_type}")
    
    try:
        if sync_type == 'cadastral':
            total_synced = await sync_cadastral()
            if total_synced is not None:
                logger.info(f"Cadastral sync completed. Total records: {total_synced:,}")
            
        elif sync_type == 'wetlands':
            total_synced = await sync_wetlands()
            if total_synced is not None:
                logger.info(f"Wetlands sync completed. Total records: {total_synced:,}")
            
        elif sync_type == 'all':
            # Run both syncs
            cadastral_total = await sync_cadastral()
            wetlands_total = await sync_wetlands()
            logger.info(f"All syncs completed. Cadastral: {cadastral_total:,}, Wetlands: {wetlands_total:,}")
        
        else:
            logger.error(f"Unknown sync type: {sync_type}")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Sync failed: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    success = asyncio.run(run_sync())
    exit(0 if success else 1)
