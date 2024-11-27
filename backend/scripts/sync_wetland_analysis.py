import asyncio
import os
import sys
from pathlib import Path
import asyncpg
from dotenv import load_dotenv
import logging
import signal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Add the backend directory to Python path
backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))

from src.sources.parsers.wetland_analysis import WetlandAnalysis
from src.config import SOURCES

# Add graceful shutdown
shutdown = asyncio.Event()

def handle_shutdown(signum, frame):
    logger.info(f"Received signal {signum}. Starting graceful shutdown...")
    shutdown.set()

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

async def main() -> int:
    """Run the wetland analysis"""
    load_dotenv()
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'ssl': os.getenv('DB_SSL', 'require'),
        'command_timeout': 7200  # 2 hours
    }
    
    try:
        pool = await asyncpg.create_pool(**db_config)
        logger.info("Database connection established")
        
        async with pool.acquire() as conn:
            analyzer = WetlandAnalysis(SOURCES["wetland_analysis"])
            total_analyzed = await analyzer.sync(conn)
            logger.info(f"Analysis completed. Total records: {total_analyzed:,}")
            return total_analyzed
            
    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}")
        raise
    finally:
        if 'pool' in locals():
            await pool.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        sys.exit(1) 