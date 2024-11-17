# Agricultural Data Frontend

Interactive map visualization of Danish agricultural and environmental data.

## Features
- Interactive map showing:
  - Agricultural fields
  - Wetland areas
- Real-time data updates
- Layer controls
- Data filtering

## Tech Stack
- React 18
- MapLibre GL JS
- TypeScript
- Tailwind CSS

## Development Setup

### Requirements
- Node.js 16+
- npm or yarn

### Installation Steps
1. Install dependencies: `npm install`
2. Start development server: `npm start`
3. Visit http://localhost:3000

## Project Structure

src/
├── components/ # React components
│ ├── Map/ # Map-related components
│ └── UI/ # UI components
├── api/ # Backend API client
├── hooks/ # Custom React hooks
└── types/ # TypeScript 


## API Integration

Example of fetching data:
typescript
import { fetchSources, fetchSourceData } from '../api/sources';
// List available sources
const sources = await fetchSources();
// Fetch specific source data
const data = await fetchSourceData('agricultural_fields');

## Adding New Features
1. Create component in appropriate directory
2. Add API integration if needed
3. Update map layers if required
4. Add to main App.tsx

## Environment Variables
Required in `.env`:
- REACT_APP_API_URL: Backend API URL