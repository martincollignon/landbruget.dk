import asyncio
import os
from pathlib import Path
import sys
from supabase import create_client, Client
from dotenv import load_dotenv

# Add the backend directory to Python path
backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))

from src.sources.parsers.cadastral import Cadastral
from src.config import SOURCES

async def main():
    """Sync cadastral data to Supabase"""
    load_dotenv()
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY environment variables")
    
    try:
        # Initialize Supabase client
        supabase: Client = create_client(supabase_url, supabase_key)
        print("Database connection established")
        
        cadastral = Cadastral(SOURCES["cadastral"])
        # Modify the sync method to use supabase client instead of asyncpg
        await cadastral.sync(supabase)
        
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())