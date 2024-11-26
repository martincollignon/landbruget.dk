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

# Set debug level for cadastral module
logging.getLogger('src.sources.parsers.cadastral').setLevel(logging.DEBUG)

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
        'ssl': os.getenv('DB_SSL', 'require'),
        'command_timeout': 14400,  # 4 hours (matching task_timeout)
        'server_settings': {
            'tcp_keepalives_idle': '30',
            'tcp_keepalives_interval': '10',
            'tcp_keepalives_count': '3',
            'statement_timeout': '14400000',   # 4 hours in milliseconds
            'idle_in_transaction_session_timeout': '14400000'  # 4 hours
        }
    }
    
    if not all([db_config['host'], db_config['database'], 
                db_config['user'], db_config['password']]):
        raise ValueError("Missing database configuration")
    
    conn = None
    try:
        # Create connection pool instead of single connection
        pool = await asyncpg.create_pool(
            min_size=3,
            max_size=10,
            **db_config
        )
        logger.info("Database connection pool established")
        
        async with pool.acquire() as conn:
            cadastral = Cadastral(SOURCES["cadastral"])
            total_synced = await cadastral.sync(conn)
            logger.info(f"Total records synced: {total_synced:,}")
            return total_synced
        
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
