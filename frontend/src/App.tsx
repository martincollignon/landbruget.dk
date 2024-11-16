import React, { useEffect, useRef } from 'react';
import maplibregl, { Map, Popup } from 'maplibre-gl';
import axios from 'axios';
import 'maplibre-gl/dist/maplibre-gl.css';
import './App.css';
import { FeatureCollection } from 'geojson';

interface APIResponse {
  data: FeatureCollection;
}

function App() {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<Map | null>(null);

  useEffect(() => {
    if (map.current) return;
    
    map.current = new Map({
        container: mapContainer.current!,
        style: 'https://demotiles.maplibre.org/style.json',
        center: [10.4, 56.0],  // Center on Denmark
        zoom: 7
    });

    map.current.on('load', async () => {
        try {
            // Load agricultural fields
            const fieldsResponse = await axios.get<APIResponse>(
                'http://localhost:8000/api/agricultural-fields'
            );
            
            // Load wetlands
            const wetlandsResponse = await axios.get<APIResponse>(
                'http://localhost:8000/api/wetlands'
            );

            if (!map.current) return;

            map.current.addSource('agricultural-fields', {
                type: 'geojson',
                data: fieldsResponse.data.data
            });

            map.current.addSource('wetlands', {
                type: 'geojson',
                data: wetlandsResponse.data.data
            });

            // Add wetlands layer
            map.current.addLayer({
                id: 'wetlands-fill',
                type: 'fill',
                source: 'wetlands',
                paint: {
                    'fill-color': '#2196F3',
                    'fill-opacity': 0.6
                }
            });

            // Add agricultural fields layer
            map.current.addLayer({
                id: 'fields-fill',
                type: 'fill',
                source: 'agricultural-fields',
                paint: {
                    'fill-color': '#4CAF50',
                    'fill-opacity': 0.4
                }
            });

            // Add hover effects
            addMapInteractivity(map.current);

        } catch (error) {
            console.error('Error loading data:', error);
        }
    });
  }, []);

  return (
    <div className="App">
      <header className="App-header">
        <h1>Danish Farms Map</h1>
      </header>
      <div ref={mapContainer} className="map-container" />
    </div>
  );
}

function addMapInteractivity(map: Map) {
    const popup = new maplibregl.Popup({
        closeButton: false,
        closeOnClick: false
    });

    map.on('mousemove', 'fields-fill', (e) => {
        if (e.features && e.features.length > 0) {
            const feature = e.features[0];
            
            // Change cursor style
            map.getCanvas().style.cursor = 'pointer';
            
            // Show popup
            popup.setLngLat(e.lngLat)
                .setHTML(`
                    <h4>Agricultural Field</h4>
                    <p>Crop Type: ${feature.properties?.cropType || 'Unknown'}</p>
                    <p>Area: ${feature.properties?.area || 'Unknown'} ha</p>
                `)
                .addTo(map);
        }
    });

    map.on('mouseleave', 'fields-fill', () => {
        map.getCanvas().style.cursor = '';
        popup.remove();
    });
}

export default App;