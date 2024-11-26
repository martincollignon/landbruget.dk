import asyncio
import os
from pathlib import Path
import sys
import asyncpg
from dotenv import load_dotenv
import logging
from typing import Optional
import signal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add the backend directory to Python path
backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))

from src.sources.parsers.water_projects import WaterProjects
from src.config import SOURCES

# Add graceful shutdown
shutdown = asyncio.Event()

def handle_shutdown(signum, frame):
    logger.info(f"Received signal {signum}. Starting graceful shutdown...")
    shutdown.set()

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

async def main() -> Optional[int]:
    """Sync water projects data to PostgreSQL"""
    load_dotenv()
    
    # Get database configuration
    db_config = {
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'ssl': os.getenv('DB_SSL', 'require'),
        'command_timeout': 14400  # 4 hours
    }
    
    if not all([db_config['host'], db_config['database'], 
                db_config['user'], db_config['password']]):
        raise ValueError("Missing database configuration")
    
    try:
        pool = await asyncpg.create_pool(
            min_size=3,
            max_size=10,
            **db_config
        )
        logger.info("Database connection pool established")
        
        async with pool.acquire() as conn:
            config = SOURCES["water_projects"].copy()
            water_projects = WaterProjects(config)
            total_synced = await water_projects.sync(conn)
            logger.info(f"Total records synced: {total_synced:,}")
            return total_synced
        
    except asyncpg.PostgresError as e:
        logger.error(f"Database error during sync: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error during sync: {str(e)}")
        raise
    finally:
        if 'pool' in locals():
            await pool.close()
            logger.info("Database connection pool closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        sys.exit(1) 