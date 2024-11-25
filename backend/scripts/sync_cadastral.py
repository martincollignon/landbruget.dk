import asyncio
import os
from pathlib import Path
import sys
import asyncpg
from dotenv import load_dotenv
import logging
from typing import Optional
import signal

# Configure logging for Cloud Run
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Cloud Run logs to stdout
    ]
)
logger = logging.getLogger(__name__)

# Add the backend directory to Python path
backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))

from src.sources.parsers.cadastral import Cadastral
from src.config import SOURCES

# Add graceful shutdown
shutdown = asyncio.Event()

def handle_shutdown(signum, frame):
    logger.info(f"Received signal {signum}. Starting graceful shutdown...")
    shutdown.set()

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

async def main() -> Optional[int]:
    """Sync cadastral data to PostgreSQL"""
    load_dotenv()
    
    # Configure logging levels
    logging.getLogger('src.sources.parsers.cadastral').setLevel(logging.INFO)
    logging.getLogger('src.base').setLevel(logging.INFO)
    
    # Get database configuration from environment
    db_config = {
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'ssl': os.getenv('DB_SSL', 'require'),  # Add SSL for Cloud SQL
        'command_timeout': 60  # Add command timeout
    }
    
    if not all([db_config['host'], db_config['database'], 
                db_config['user'], db_config['password']]):
        raise ValueError("Missing database configuration")
    
    conn = None
    try:
        # Connect with SSL and timeout
        conn = await asyncpg.connect(**db_config)
        logger.info("Database connection established")
        
        cadastral = Cadastral(SOURCES["cadastral"])
        total_synced = await cadastral.sync(conn)
        logger.info(f"Total records synced: {total_synced:,}")
        return total_synced
        
    except Exception as e:
        logger.error(f"Error during sync: {str(e)}")
        raise
    finally:
        if conn:
            await conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        sys.exit(1)
