import pandas as pd

def process_data():
    # Replace this with actual data loading and processing
    # For example, read data from a CSV file
    data = pd.DataFrame({
        'name': ['Farm A', 'Farm B'],
        'latitude': [55.6761, 56.2639],
        'longitude': [12.5683, 9.5018],
    })

    # Convert DataFrame to GeoJSON format
    features = []
    for _, row in data.iterrows():
        features.append({
            "type": "Feature",
            "properties": {
                "name": row['name'],  # Adjust based on your data columns
            },
            "geometry": {
                "type": "Point",
                "coordinates": [row['longitude'], row['latitude']]
            }
        })

    geojson_data = {
        "type": "FeatureCollection",
        "features": features
    }

    return geojson_data
