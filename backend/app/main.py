from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
from datetime import datetime
import os

from app.data_sources.wfs_source import WFSDataSource
from app.data_sources.base import DataSourceError
from app.config.sources import get_source_config
from app.data_sources.shapefile_source import ShapefileDataSource
from app.storage import GeoParquetStorage  # Fixed import
from app.pipeline import Pipeline
from app.transformers.geojson import GeoJSONToGeoDataFrame
from app.processors.optimize import OptimizeDataTypes
from app.utils.scheduling import get_next_monday_timestamp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Agricultural Data API",
    description="API serving agricultural geospatial data from various sources",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/sources")
async def list_sources():
    """List all available data sources"""
    from app.config.sources import SOURCES
    
    return {
        source_id: {
            "name": config.name,
            "type": config.type,
            "description": config.description,
            "enabled": config.enabled
        }
        for source_id, config in SOURCES.items()
        if config.enabled
    }

@app.get("/api/agricultural-fields")
async def get_agricultural_fields():
    """Get agricultural field data from Danish Agricultural Agency WFS"""
    source = None
    try:
        config = get_source_config("agricultural_fields")
        source = WFSDataSource(config.dict())
        result = await source.fetch_data()
        
        return JSONResponse(
            content=result.data,
            headers={
                'X-Last-Updated': result.timestamp.isoformat(),
                'X-Next-Update': get_next_monday_timestamp(),
                'X-Update-Frequency': 'weekly'
            }
        )
    except Exception as e:
        logger.error(f"Error fetching agricultural fields: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if source:
            await source.close()

@app.get("/api/wetlands")
async def get_wetlands():
    """Get wetlands map data"""
    source = None
    try:
        storage = GeoParquetStorage(
            bucket_name=os.getenv('GCS_BUCKET', 'landbrugsdata-1-data'),
            project_id=os.getenv('GOOGLE_CLOUD_PROJECT', 'landbrugsdata-1')
        )
        
        config = get_source_config("wetlands_map")
        source = ShapefileDataSource(config.dict())
        
        pipeline = Pipeline(
            source=source,
            storage=storage,
            transformers=[GeoJSONToGeoDataFrame()],
            processors=[OptimizeDataTypes()]
        )
        
        result = await pipeline.execute()
        return JSONResponse(content={"path": result})
    except Exception as e:
        logger.error(f"Error fetching wetlands data: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if source:
            await source.close()