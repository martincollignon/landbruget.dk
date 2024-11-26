from fastapi import FastAPI
from scripts.sync_cadastral import main as sync_cadastral
from scripts.sync_wetlands import main as sync_wetlands
from scripts.sync_water_projects import main as sync_water_projects

app = FastAPI(
    title="Data Sync Service",
    description="Service for syncing various geographical data sources"
)

@app.post("/sync/{source_type}")
async def run_sync(source_type: str):
    """Endpoint to trigger data sync"""
    try:
        if source_type == "cadastral":
            await sync_cadastral()
        elif source_type == "wetlands":
            await sync_wetlands()
        elif source_type == "water_projects":
            await sync_water_projects()
        else:
            return {"status": "error", "message": f"Unknown source type: {source_type}"}
            
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}
