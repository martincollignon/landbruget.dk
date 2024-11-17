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

3. Static Agricultural Data
   - Animal Welfare: Inspection reports and focus areas
   - Biogas: Production data, support schemes, and methane leakage reports
   - Fertilizer: Nitrogen data and climate tool calculations
   - Herd Data: CHR (Central Husbandry Register) data
   - Pesticides: Usage statistics (2021-2023)
   - Pig Movements: International transport data (2017-2024)
   - Subsidies: Agricultural support schemes and project grants
   - Visa: Agricultural visa statistics

## Project Structure
<pre>
├── backend/
│   ├── src/
│   │   ├── sources/
│   │   │   ├── base.py
│   │   │   ├── parsers/           # API/WFS sources
│   │   │   │   ├── agricultural_fields/
│   │   │   ├── static/            # Static data files
│   │   │   │   ├── animal_welfare/
│   │   │   │   ├── biogas/
│   │   │   │   ├── fertilizer/
│   │   │   │   ├── herd_data/
│   │   │   │   ├── pesticides/
│   │   │   │   ├── pig_movements/
│   │   │   │   ├── subsidies/
│   │   │   │   ├── visa/
│   │   │   │   └── wetlands/
│   │   ├── main.py
│   │   └── config.py
└── frontend/
    └── src/
        ├── components/
        └── api/
</pre>   
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

## Data Updates
- WFS sources: Automatic weekly updates (Mondays at 2 AM UTC)
- Static sources: Manual updates through pull requests
- All updates are automatically deployed to Google Cloud Run
