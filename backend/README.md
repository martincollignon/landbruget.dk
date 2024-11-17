# Agricultural Data API

Backend service for Danish agricultural and environmental data.

## Architecture

.
├── backend/
│   └── src/
│       ├── sources/
│       │   ├── base.py
│       │   ├── parsers/          # API/WFS sources
│       │   │   └── agricultural_fields/
│       │   └── static/           # Static file sources
│       │       ├── animal_welfare/
│       │       ├── biogas/
│       │       ├── fertilizer/
│       │       ├── herd_data/
│       │       ├── pesticides/
│       │       ├── pig_movements/
│       │       ├── subsidies/
│       │       ├── visa/
│       │       └── wetlands/
│       ├── main.py
│       └── config.py
└── frontend/
    └── src/
        ├── components/
        └── api/
        
## API Endpoints

- `GET /health` - Health check
- `GET /sources` - List available data sources
- `GET /sources/{source_id}` - Get data for specific source

## Development Setup

### Requirements
- Python 3.9+
- GDAL library
- Virtual environment

### Installation Steps
1. Create and activate virtual environment
2. Install dependencies: `pip install -r requirements.txt`
3. Run development server: `uvicorn src.main:app --reload`

## Adding New Data Sources


1. Choose location:
   - `sources/parsers/` for API/WFS sources
   - `sources/static/` for static file sources

2. Implement your parser:

from ...base import Source
class YourSource(Source):
async def fetch(self) -> pd.DataFrame:
# Implement data fetching
pass

3. Add to `config.py`:
python
SOURCES = {
"your_source": {
"name": "Your Source Name",
"type": "wfs", # or "static"
"enabled": True,
# Add source-specific config
}
}
## Environment Variables
Required in `.env`:
- GOOGLE_CLOUD_PROJECT: Your GCP project ID
- GCS_BUCKET: Your GCS bucket name

## Deployment
Automatic deployment to Google Cloud Run:
- On push to main branch
- Weekly on Mondays at 2 AM UTC
