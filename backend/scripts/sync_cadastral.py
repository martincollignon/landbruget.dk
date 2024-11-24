import asyncio
import os
from pathlib import Path
import sys
import asyncpg
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the backend directory to Python path
backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))

from src.sources.parsers.cadastral import Cadastral
from src.config import SOURCES

async def main():
    """Sync cadastral data to PostgreSQL"""
    load_dotenv()
    
    # Add debug logging
    logging.getLogger('src.sources.parsers.cadastral').setLevel(logging.DEBUG)
    logging.getLogger('src.base').setLevel(logging.DEBUG)
    
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
        
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise
    finally:
        if conn:
            await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
