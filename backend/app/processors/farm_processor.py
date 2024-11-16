import pandas as pd
from app.processors.base import GeoDataProcessor
from app.config.data_sources import DATA_SOURCES

class FarmDataProcessor(GeoDataProcessor):
    """Processor for farm location data."""
    
    def process(self) -> dict:
        """Process farm data from Excel file into GeoJSON."""
        config = DATA_SOURCES["farms"]
        
        try:
            # Explicitly specify the Excel engine
            df = pd.read_excel(
                config.path,
                engine='openpyxl',
                sheet_name=0  # Read first sheet
            )
            
            # Verify the required columns exist
            required_columns = ['name', 'latitude', 'longitude']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            features = []
            for _, row in df.iterrows():
                properties = {
                    "name": str(row['name']),  # Convert to string to ensure it's serializable
                }
                coordinates = (float(row['longitude']), float(row['latitude']))
                features.append(self.create_feature(properties, coordinates))

            return {
                "type": "FeatureCollection",
                "features": features
            }
            
        except FileNotFoundError:
            print(f"Excel file not found at path: {config.path}")
            raise
        except Exception as e:
            print(f"Error processing farm data: {str(e)}")
            print(f"File path attempted: {config.path}")
            raise