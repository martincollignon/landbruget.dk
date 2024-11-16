from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.processors.farm_processor import FarmDataProcessor

app = FastAPI(
    title="GeoData API",
    description="API for serving processed geospatial data",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROCESSORS = {
    "farms": FarmDataProcessor(),
}

@app.get("/api/geojson/{source}")
async def get_geojson(source: str):
    """Get GeoJSON data for a specific source."""
    processor = PROCESSORS.get(source)
    if not processor:
        raise HTTPException(
            status_code=404,
            detail=f"Data source '{source}' not found"
        )
    
    try:
        geojson_data = processor.process()
        return JSONResponse(content=geojson_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))