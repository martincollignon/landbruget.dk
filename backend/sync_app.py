import asyncio
import logging
from scripts.sync_cadastral import main as sync_cadastral

# Configure logging with more detail
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_sync():
    """Run the cadastral sync process"""
    logger.info("Starting cadastral sync...")
    try:
        await sync_cadastral()
        logger.info("Sync completed successfully")
        return True
    except Exception as e:
        logger.error(f"Sync failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = asyncio.run(run_sync())
    exit(0 if success else 1)
