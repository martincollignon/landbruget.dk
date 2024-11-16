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
        center: [0, 0],
        zoom: 2,
      });

      map.current.on('load', () => {
        axios
          .get<GeoJSON.FeatureCollection>('http://localhost:8000/api/geojson')
          .then((response) => {
            map.current?.addSource('backend-data', {
              type: 'geojson',
              data: response.data,
            });

            map.current?.addLayer({
              id: 'backend-layer',
              type: 'circle',
              source: 'backend-data',
              paint: {
                'circle-radius': 6,
                'circle-color': '#0000FF',
              },
            });

            // Add click event for pop-ups
            map.current?.on('click', 'backend-layer', (e) => {
              const features = e.features as maplibregl.MapGeoJSONFeature[];
              if (!features || !features.length) return;

              const coordinates = (features[0].geometry as GeoJSON.Point).coordinates.slice();
              const name = features[0].properties?.name;

              // Ensure the popup appears over the correct copy
              while (Math.abs(e.lngLat.lng - coordinates[0]) > 180) {
                coordinates[0] += e.lngLat.lng > coordinates[0] ? 360 : -360;
              }

              new Popup()
                .setLngLat(coordinates as [number, number])
                .setHTML(`<strong>${name}</strong>`)
                .addTo(map.current!);
            });

            // Change the cursor to a pointer when over the markers
            map.current?.on('mouseenter', 'backend-layer', () => {
              map.current!.getCanvas().style.cursor = 'pointer';
            });
            map.current?.on('mouseleave', 'backend-layer', () => {
              map.current!.getCanvas().style.cursor = '';
            });
          })
          .catch((error) => {
            console.error('Error fetching GeoJSON data:', error);
          });
      });
    }
  }, []);

  return (
    <div className="App">
      <div ref={mapContainer} className="map-container" />
    </div>
  );
}

export default App;
