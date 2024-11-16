# Landbrugsdata Project

This project is an open-source initiative for citizen science in the agricultural sector. It consists of:

- **Front-End**: A React application using TypeScript and MapLibre GL JS for map visualization.
- **Back-End**: A FastAPI server providing API endpoints to serve GeoJSON data.

## Project Structure

- `frontend/`: Contains the React front-end code.
- `backend/`: Contains the FastAPI back-end code.

## Goals

- Visualize agricultural data on an interactive map.
- Fetch and display GeoJSON data from the back-end API.
- Implement interactivity such as pop-ups and data filtering.

## How to Run

- **Back-End**:
  - Activate the virtual environment: `source venv/bin/activate`
  - Run the server: `uvicorn main:app --reload`
- **Front-End**:
  - Install dependencies: `npm install`
  - Start the development server: `npm start`

