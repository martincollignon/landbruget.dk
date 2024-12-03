import asyncio
import logging
import os
from scripts.sync_cadastral import main as sync_cadastral
from scripts.sync_wetlands import main as sync_wetlands
from scripts.sync_water_projects import main as sync_water_projects
from scripts.sync_agricultural_fields import main as sync_agricultural_fields

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
            
        elif sync_type == 'water_projects':
            total_synced = await sync_water_projects()
            if total_synced is not None:
                logger.info(f"Water projects sync completed. Total records: {total_synced:,}")
            
        elif sync_type == 'agricultural_fields':
            total_synced = await sync_agricultural_fields()
            if total_synced is not None:
                logger.info(f"Agricultural fields sync completed. Total records: {total_synced:,}")
            
        elif sync_type == 'all':
            # Run all syncs in the correct order
            cadastral_total = await sync_cadastral()
            wetlands_total = await sync_wetlands()
            water_projects_total = await sync_water_projects()
            agricultural_fields_total = await sync_agricultural_fields()
            logger.info(f"All syncs completed. Cadastral: {cadastral_total:,}, "
                       f"Wetlands: {wetlands_total:,}, "
                       f"Water Projects: {water_projects_total:,}, "
                       f"Agricultural Fields: {agricultural_fields_total:,}")
        
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
