import asyncio
import os
from pathlib import Path
import sys
import logging
from typing import Optional
import signal
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))

from src.sources.parsers.herd_data import HerdDataParser
from src.config import SOURCES

shutdown = asyncio.Event()

def handle_shutdown(signum, frame):
    logger.info(f"Received signal {signum}. Starting graceful shutdown...")
    shutdown.set()

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

async def main() -> Optional[int]:
    """Sync herd data to Cloud Storage"""
    load_dotenv()
    try:
        herd_data = HerdDataParser(SOURCES["herd_data"])
        total_synced = await herd_data.sync()  # Now awaiting the async call
        if total_synced is not None:
            logger.info(f"Total records synced: {total_synced:,}")
        else:
            logger.warning("No records were synced")
        return total_synced
    except Exception as e:
        logger.error(f"Error during sync: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        sys.exit(1) 