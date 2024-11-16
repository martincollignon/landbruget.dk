# Danish Agricultural & Environmental Data Visualization

A citizen science initiative for visualizing and analyzing Danish agricultural and environmental data.

## Features

- Interactive map visualization of:
  - Agricultural fields from Danish agricultural database
  - Wetland areas from Environmental Protection Agency
- Real-time data from WFS services
- Automated data synchronization
- Cloud storage optimization

## Data Sources

- Agricultural Fields (WFS)
  - Source: Danish Agricultural Agency
  - Updates: Daily
  - Content: Field boundaries and crop types

- Wetlands Map (Static)
  - Source: Danish Environmental Protection Agency
  - Updates: Yearly
  - Content: Wetland areas and carbon content

## Getting Started

1. Backend Setup:
   ```bash
   cd backend
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   uvicorn app.main:app --reload
   ```

2. Frontend Setup:
   ```bash
   cd frontend
   npm install
   npm start
   ```

Visit http://localhost:3000 to view the map.