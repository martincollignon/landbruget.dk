A citizen science initiative for visualizing and analyzing Danish agricultural data.

## Overview

This project provides an interactive map visualization of Danish agricultural data, combining real-time data from WFS services with cloud storage for efficient data management.

## Features

- Interactive map visualization using MapLibre GL JS
- Real-time agricultural data from Danish WFS services
- Automated data synchronization pipeline
- Cloud storage integration with Google Cloud Storage

## Project Structure

- `frontend/`: React frontend application with MapLibre GL JS
- `backend/`: FastAPI backend service and data pipeline
- `.github/`: CI/CD workflows for deployment and data sync

## Setup

### Prerequisites

- Python 3.9+
- Node.js 16+
- Google Cloud Platform account
- Git

### Backend Setup

1. Create virtual environment:
    ```
    cd backend
    python -m venv venv
    source venv/bin/activate  # or `venv\Scripts\activate` on Windows
    pip install -r requirements.txt
    ```

2. Configure environment:
    ```
    cp .env.example .env
    # Edit .env with your configuration
    ```

3. Run the development server:
    ```
    uvicorn app.main:app --reload
    ```

### Frontend Setup

1. Install and run:
    ```
    cd frontend
    npm install
    npm start
    ```

## Development

- Backend API: http://localhost:8000
- Frontend: http://localhost:3000
- API documentation: http://localhost:8000/docs

## Data Pipeline

The project includes an automated data pipeline that:
- Fetches data from Danish WFS services
- Transforms and optimizes the data
- Stores it in GeoParquet format on Google Cloud Storage
- Runs daily via GitHub Actions

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request