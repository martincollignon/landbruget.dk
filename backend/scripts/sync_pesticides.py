import asyncio
import os
from pathlib import Path
import sys
import logging
from typing import Optional
from dotenv import load_dotenv

from src.sources.static.pesticides.parser import Pesticides
from src.config import SOURCES

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))


async def main() -> Optional[int]:
    """Sync pesticide data to Cloud Storage"""
    load_dotenv()
    try:
        pesticides = Pesticides(SOURCES["pesticides"])
        total_synced = await pesticides.sync()
        logger.info(f"Total records synced: {total_synced:,}")
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