from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
from datetime import datetime, timedelta

from .config import SOURCES
from .sources.parsers.agricultural_fields import AgriculturalFields
from .sources.parsers.wetlands import Wetlands
from .sources.parsers.water_projects import WaterProjects

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Danish Agricultural Data API",
    description="API serving agricultural and environmental data"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/sources")
async def list_sources():
    """List all available data sources"""
    return {
        source_id: {
            "name": config["name"],
            "type": config["type"],
            "description": config["description"],
            "frequency": config["frequency"]
        }
        for source_id, config in SOURCES.items()
        if config["enabled"]
    }

@app.get("/data/{source_id}")
async def get_data(source_id: str):
    """Get data from a specific source"""
    if source_id not in SOURCES:
        raise HTTPException(status_code=404, detail="Source not found")
        
    config = SOURCES[source_id]
    if not config["enabled"]:
        raise HTTPException(status_code=403, detail="Source is disabled")
    
    try:
        # Initialize appropriate source
        source = None
        if source_id == "agricultural_fields":
            source = AgriculturalFields(config)
        elif source_id == "wetlands":
            source = Wetlands(config)
        elif source_id == "water_projects":
            source = WaterProjects(config)
        else:
            raise HTTPException(status_code=501, detail="Source not implemented")
        
        # Fetch data
        df = await source.fetch()
        
        # Get next update time for weekly sources
        next_update = None
        if config["frequency"] == "weekly":
            next_update = (datetime.now() + timedelta(days=7)).isoformat()
        
        return JSONResponse(
            content=df.to_dict(orient="records"),
            headers={
                "X-Last-Updated": datetime.now().isoformat(),
                "X-Next-Update": next_update,
                "X-Update-Frequency": config["frequency"]
            }
        )
        
    except Exception as e:
        logger.error(f"Error fetching data from {source_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
