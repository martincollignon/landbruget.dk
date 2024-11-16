import React, { useEffect, useRef } from 'react';
import maplibregl, { Map, Popup } from 'maplibre-gl';
import axios from 'axios';
import 'maplibre-gl/dist/maplibre-gl.css';
import './App.css';

function App() {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<Map | null>(null);

  useEffect(() => {
    if (map.current) return;

    if (mapContainer.current) {
      map.current = new maplibregl.Map({
        container: mapContainer.current,
        style: 'https://demotiles.maplibre.org/style.json',
        center: [10.4234, 55.9987],
        zoom: 7,
      });

      map.current.on('load', async () => {
        try {
          console.log('Map loaded, fetching data...');
          
          const response = await axios.get<GeoJSON.FeatureCollection>(
            'http://localhost:8000/api/geojson/farms'
          );
          
          console.log('Data received:', response.data);

          if (!map.current) return;

          // Add source
          map.current.addSource('farms', {
            type: 'geojson',
            data: response.data,
          });

          // Add circle layer
          map.current.addLayer({
            id: 'farms-circles',
            type: 'circle',
            source: 'farms',
            paint: {
              'circle-radius': 8,
              'circle-color': '#2E7D32',
              'circle-stroke-width': 2,
              'circle-stroke-color': '#fff',
            },
          });

          // Add labels layer
          map.current.addLayer({
            id: 'farms-labels',
            type: 'symbol',
            source: 'farms',
            layout: {
              'text-field': ['get', 'name'],
              'text-offset': [0, 1.5],
              'text-anchor': 'top',
              'text-size': 12,
            },
            paint: {
              'text-color': '#000',
              'text-halo-width': 1,
              'text-halo-color': '#fff',
            },
          });

          // Add hover effect
          map.current.on('mouseenter', 'farms-circles', () => {
            if (map.current) {
              map.current.getCanvas().style.cursor = 'pointer';
            }
          });

          map.current.on('mouseleave', 'farms-circles', () => {
            if (map.current) {
              map.current.getCanvas().style.cursor = '';
            }
          });

          // Add click popup
          map.current.on('click', 'farms-circles', (e) => {
            if (!e.features?.length || !map.current) return;

            const feature = e.features[0];
            const coordinates = (feature.geometry as GeoJSON.Point).coordinates.slice() as [number, number];
            const properties = feature.properties;

            // Only create and add popup if map exists
            if (map.current) {
              new Popup()
                .setLngLat(coordinates)
                .setHTML(`
                  <div style="padding: 10px;">
                    <h3 style="margin: 0 0 10px 0;">${properties?.name}</h3>
                    <p style="margin: 0;">Latitude: ${coordinates[1].toFixed(4)}</p>
                    <p style="margin: 0;">Longitude: ${coordinates[0].toFixed(4)}</p>
                  </div>
                `)
                .addTo(map.current);
            }
          });


        } catch (error) {
          console.error('Error fetching GeoJSON data:', error);
        }
      });
    }
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

export default App;