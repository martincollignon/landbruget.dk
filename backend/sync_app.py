from fastapi import FastAPI
from scripts.sync_cadastral import main as sync_cadastral

app = FastAPI(
    title="Cadastral Sync Service",
    description="Service for syncing cadastral data"
)

@app.post("/")
async def run_sync():
    """Endpoint to trigger cadastral sync"""
    try:
        await sync_cadastral()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}
