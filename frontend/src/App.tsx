import { useEffect, useRef } from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import './App.css';

function App() {
    const mapContainer = useRef<HTMLDivElement>(null);
    const map = useRef<maplibregl.Map | null>(null);

    useEffect(() => {
        if (map.current || !mapContainer.current) return;

        map.current = new maplibregl.Map({
            container: mapContainer.current,
            style: 'https://demotiles.maplibre.org/style.json',
            center: [10.4, 56.0],  // Center on Denmark
            zoom: 7
        });

        return () => {
            map.current?.remove();
        };
    }, []);

    return (
        <div className="App">
            <header className="App-header">
                <h1>Danish Agricultural Data</h1>
            </header>
            <div ref={mapContainer} className="map-container" />
        </div>
    );
}

export default App;
