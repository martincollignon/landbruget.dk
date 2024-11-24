import asyncio
import os
from pathlib import Path
import sys
import asyncpg
from dotenv import load_dotenv

# Add the backend directory to Python path
backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))

from src.sources.parsers.cadastral import Cadastral
from src.config import SOURCES

async def main():
    """Sync cadastral data to PostgreSQL"""
    load_dotenv()
    
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    if not all([db_host, db_name, db_user, db_password]):
        raise ValueError("Missing database configuration")
    
    try:
        # Connect to PostgreSQL using Unix socket
        conn = await asyncpg.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
        print("Database connection established")
        
        cadastral = Cadastral(SOURCES["cadastral"])
        await cadastral.sync(conn)
        await conn.close()
        
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
