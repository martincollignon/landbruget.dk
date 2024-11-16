from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
import logging

from app.data_sources.excel_source import ExcelDataSource
from app.processors.farm_processor import FarmProcessor
from app.storage.base import Storage  # We'll implement GCP storage later
from app.pipeline import Pipeline

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="GeoData API",
    description="API for serving processed geospatial data",
    version="1.0.0"
)

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Welcome to GeoData API"}

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "version": "1.0.0"
    }

@app.get("/api/geojson/farms")
async def get_farms():
    try:
        # Create pipeline components
        source = ExcelDataSource({
            'file_path': 'app/data/farms.xlsx',
            'crs': 'EPSG:4326'
        })
        
        processor = FarmProcessor()
        
        # Create and run pipeline
        pipeline = Pipeline(
            name='farms',
            source=source,
            transformers=[],  # Add transformers if needed
            processors=[processor],
            storage=None  # We'll add storage later
        )
        
        return pipeline.run()
        
    except Exception as e:
        logger.error(f"Error processing farms request: {e}")
        raise
