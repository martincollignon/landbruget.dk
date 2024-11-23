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
    """Sync cadastral data to Supabase"""
    load_dotenv()
    
    # Connect to Supabase
    conn = await asyncpg.connect(
        user='postgres',
        password=os.getenv('SUPABASE_KEY'),
        database='postgres',
        host=os.getenv('SUPABASE_URL').replace('https://', '').replace('.supabase.co', '.supabase.co'),
        port=5432,
        ssl='require'
    )
    
    try:
        cadastral = Cadastral(SOURCES["cadastral"])
        await cadastral.sync(conn)
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())