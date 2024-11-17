# Danish Agricultural & Environmental Data Visualization

Interactive visualization of Danish agricultural and environmental data.

## Overview

This project provides:
- Backend API serving agricultural and environmental data from Danish government sources
- Frontend map visualization of the data
- Automated weekly data synchronization

## Data Sources

1. Agricultural Fields (WFS)
   - Source: Danish Agricultural Agency
   - Updates: Weekly (Mondays at 2 AM UTC)
   - Content: Field boundaries and crop types

2. Wetlands Map (Static)
   - Source: Danish Environmental Protection Agency
   - Updates: Static dataset
   - Content: Wetland areas and carbon content

## Project Structure
.
├── backend/ # FastAPI backend
│ ├── src/
│ │ ├── sources/ # Data source implementations
│ │ ├── main.py # FastAPI application
│ │ └── config.py # Configuration
│ └── README.md # Backend documentation
└── frontend/ # React frontend
├── src/
│ ├── components/ # React components
│ └── api/ # API client
└── README.md # Frontend documentation

## Quick Start

See backend/README.md and frontend/README.md for detailed setup instructions.

Visit:
- Frontend: http://localhost:3000
- API docs: http://localhost:8000/docs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request