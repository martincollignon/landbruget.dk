# Landbrugsdata

The purpose of this project is to organize information about the Danish agricultural sector and make it universally accessible and useful.

![Backend](https://img.shields.io/badge/backend-Python%203.9-green.svg)
![Frontend](https://img.shields.io/badge/frontend-React%2018-blue.svg)
![License](https://img.shields.io/badge/license-CC--BY-green.svg)
[![slack](https://img.shields.io/badge/slack-landbrugsdata-brightgreen.svg?logo=slack)](https://join.slack.com/t/landbrugsdata/shared_invite/zt-2unmop7jo-eig_P_ThEfi~A395tR_ySA)

## 🚀 Quick Start

### Backend Setup

```bash
cd backend
python -m venv venv
source venv/bin/activate # or venv\Scripts\activate on Windows
pip install -r requirements.txt
uvicorn src.main:app --reload
```

### Frontend Setup

```bash
cd frontend
npm install
npm start
```

Visit:
- Frontend: http://localhost:3000
- API Documentation: http://localhost:8000/docs

## 🏗️ Architecture

### Backend (FastAPI + Python)
<pre>
├── src/
│ ├── sources/ # Data source implementations
│ │ ├── base.py # Base source class
│ │ ├── parsers/ # API/WFS sources
│ │ └── static/ # Static file sources
│ ├── main.py # FastAPI application
│ └── config.py # Configuration
</pre>


### Frontend (React + TypeScript)

<pre>
frontend/
└── src/
├── components/ # React components
├── api/ # Backend API client
├── hooks/ # Custom React hooks
└── types/ # TypeScript definitions
</pre>

## 📊 Data Sources

Data should have one or more of the following attributes to be useful:
- CVR number (company registration number)
- CHR number (herd registration number)
- geospatial coordinates (point or polygon)
- enhedsnummer (CVR individual identifier)
- bfe number (cadaster number)

### Live Sources
1. **Agricultural Fields (WFS)**
   - Updates: Weekly (Mondays 2 AM UTC)
   - Content: Field boundaries, crop types

### Static Sources
All static sources are updated through manual pull requests:
- Animal Welfare: Inspection reports and focus areas
- Biogas: Production data and methane leakage reports
- Fertilizer: Nitrogen data and climate calculations
- Herd Data: CHR (Central Husbandry Register)
- Pesticides: Usage statistics (2010-2023)
- Pig Movements: International transport (2017-2024)
- Subsidies: Support schemes and project grants
- Visa: Agricultural visa statistics
- Wetlands: Areas and carbon content

## 🤝 Contributing

### Prerequisites
- Python 3.9+
- Node.js 16+
- GDAL library
- Git

### Development Workflow
1. Fork the repository
2. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Make your changes
4. Run tests:
   ```bash
   # Backend
   cd backend
   pytest

   # Frontend
   cd frontend
   npm test
   ```
5. Submit a pull request

### Adding New Data Sources
1. Choose the appropriate directory:
   - `backend/src/sources/parsers/` for API/WFS sources
   - `backend/src/sources/static/` for static files

2. Create a new parser class:
   ```python
   from ..base import Source

   class YourSource(Source):
       async def fetch(self) -> pd.DataFrame:
           # Implement data fetching
           pass
   ```

3. Add configuration in `backend/src/config.py`
4. Update frontend types in `frontend/src/types/`
5. Add visualization in `frontend/src/components/`

## 🔄 Data Updates
- **WFS Sources**: Automatic weekly updates (Mondays 2 AM UTC)
- **Static Sources**: Manual updates via pull requests
- **Deployment**: Automatic to Google Cloud Run

## 🔧 Environment Setup

### Backend (.env)

```
GOOGLE_CLOUD_PROJECT=your-project-id
GCS_BUCKET=your-bucket-name
```

### Frontend (.env)

```
REACT_APP_API_URL=http://localhost:8000
```

## 📚 Documentation
- [Backend API Documentation](http://localhost:8000/docs)
- [Frontend Component Documentation](frontend/README.md)
- [Data Source Specifications](backend/README.md)

## 🐛 Troubleshooting

Common issues and solutions:

1. **GDAL Installation**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install gdal-bin libgdal-dev

   # macOS
   brew install gdal
   ```

2. **API Connection Issues**
   - Verify backend is running
   - Check CORS settings
   - Confirm environment variables

## 📝 License
This work is licensed under a Creative Commons Attribution 4.0 International License (CC-BY).

## 🙏 Acknowledgments
- Danish Agricultural Agency
- Danish Environmental Protection Agency
- Danish Energy Agency
- SIRI
