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

@app.get("/api/wfs/fvm/markers")
async def get_fvm_markers():
    """Get marker data from FVM WFS"""
    source = None
    try:
        config = get_source_config("wfs_fvm_markers")
        source = WFSDataSource(config.dict())
        
        result = await source.fetch_data()
        
        # Raw GeoJSON is already JSON-serializable
        return JSONResponse(
            content=result.data,
            headers={
                'X-Feature-Count': str(result.metadata.get('feature_count', 0)),
                'X-Source-Updated': result.timestamp.isoformat()
            }
        )
        
    except DataSourceError as e:
        logger.error(f"Data source error: {str(e)}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if source:
            await source.close()

@app.get("/api/wfs/fvm/markers/metadata")
async def get_fvm_markers_metadata():
    """Get metadata about the FVM markers dataset"""
    source = None
    try:
        config = get_source_config("wfs_fvm_markers")
        source = WFSDataSource(config.dict())
        
        result = await source.fetch_data()
        
        return {
            "status": "success",
            "source": "wfs_fvm_markers",
            "name": config.name,
            "description": config.description,
            "feature_count": result.metadata.get('feature_count', 0),
            "layer": result.metadata.get('layer'),
            "last_updated": result.timestamp.isoformat()
        }
        
    except DataSourceError as e:
        logger.error(f"Data source error: {str(e)}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if source:
            await source.close()

@app.get("/api/carbon-map")
async def get_carbon_map():
    """Get carbon map data"""
    source = None
    try:
        # Initialize components
        storage = GeoParquetStorage(
            bucket_name=os.getenv('GCS_BUCKET', 'landbrugsdata-geodata'),
            project_id=os.getenv('GOOGLE_CLOUD_PROJECT', 'landbrugsdata-1')
        )
        
        config = get_source_config("carbon_map")
        source = ShapefileDataSource(config.dict())
        
        # Create pipeline
        pipeline = Pipeline(
            source=source,
            storage=storage,
            transformers=[GeoJSONToGeoDataFrame()],
            processors=[OptimizeDataTypes()]
        )
        
        # Execute pipeline
        result = await pipeline.execute()
        
        return JSONResponse(
            content={"path": result},
            headers={
                'X-Source-Updated': datetime.now().isoformat()
            }
        )
        
    except DataSourceError as e:
        logger.error(f"Data source error: {str(e)}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if source:
            await source.close()