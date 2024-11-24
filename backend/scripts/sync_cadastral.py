import asyncio
import os
from pathlib import Path
import sys
import asyncpg
from dotenv import load_dotenv
import logging
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the backend directory to Python path
backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))

from src.sources.parsers.cadastral import Cadastral
from src.config import SOURCES

async def main() -> Optional[int]:
    """Sync cadastral data to PostgreSQL"""
    load_dotenv()
    
    # Configure logging levels
    logging.getLogger('src.sources.parsers.cadastral').setLevel(logging.INFO)
    logging.getLogger('src.base').setLevel(logging.INFO)
    
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    if not all([db_host, db_name, db_user, db_password]):
        raise ValueError("Missing database configuration")
    
    conn = None
    try:
        conn = await asyncpg.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
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
    asyncio.run(main())
