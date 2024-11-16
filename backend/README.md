# Agricultural Data API

Backend service for accessing and processing agricultural geospatial data.

## Data Sources

The API supports multiple data sources:

- WFS (Web Feature Service)
  - Danish Agricultural Markers (`wfs_fvm_markers`)
  - More sources to be added...

## API Endpoints

- `GET /health` - Health check
- `GET /api/sources` - List available data sources
- `GET /api/wfs/fvm/markers` - Get Danish agricultural markers
- `GET /api/wfs/fvm/markers/metadata` - Get metadata about markers dataset

## Configuration

Data sources are configured in `app/config/sources.py`. Each source has:
- Unique identifier (e.g., `wfs_fvm_markers`)
- Type (e.g., `wfs`, `excel`)
- Connection details (URL, layer, etc.)
- Metadata (name, description)

## Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the server:
```bash
uvicorn app.main:app --reload
```
    
3. Access the API documentation:
- OpenAPI UI: http://localhost:8000/docs   
- ReDoc: http://localhost:8000/redoc