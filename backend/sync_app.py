import asyncio
import logging
from scripts.sync_cadastral import main as sync_cadastral

# Configure logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_sync() -> bool:
    """Run the cadastral sync process"""
    logger.info("Starting cadastral sync...")
    try:
        total_synced = await sync_cadastral()
        if total_synced is not None:
            logger.info(f"Sync completed successfully. Total records: {total_synced:,}")
        else:
            logger.warning("Sync completed but no records were processed")
        return True
    except Exception as e:
        logger.error(f"Sync failed: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    success = asyncio.run(run_sync())
    exit(0 if success else 1)
