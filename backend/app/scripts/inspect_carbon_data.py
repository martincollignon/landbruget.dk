import aiohttp
import zipfile
import io
import logging
from pathlib import Path
import json
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def inspect_carbon_zip():
    url = "https://www2.mst.dk/Udgiv/web/kulstof2022.zip"
    temp_dir = Path("temp_carbon")
    
    try:
        # Create temp directory
        temp_dir.mkdir(exist_ok=True)
        
        # Download the ZIP file
        logger.info(f"Downloading ZIP from {url}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download: {response.status}")
                content = await response.read()
        
        # Extract files
        logger.info("Extracting ZIP contents")
        with zipfile.ZipFile(io.BytesIO(content)) as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Convert shapefile to GeoJSON using ogr2ogr
        shp_path = temp_dir / "kulstof2022.shp"
        geojson_path = temp_dir / "kulstof2022.geojson"
        
        logger.info("\nConverting shapefile to GeoJSON...")
        result = subprocess.run([
            'ogr2ogr',
            '-f', 'GeoJSON',
            str(geojson_path),
            str(shp_path)
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Conversion failed: {result.stderr}")
            raise Exception("Failed to convert shapefile")
        
        # Read and inspect the GeoJSON
        logger.info("\nReading GeoJSON contents:")
        with open(geojson_path) as f:
            data = json.load(f)
            
        # Print information about the dataset
        logger.info(f"\nDataset Info:")
        logger.info(f"Type: {data['type']}")
        logger.info(f"Number of features: {len(data['features'])}")
        
        if data['features']:
            first_feature = data['features'][0]
            logger.info("\nProperties in first feature:")
            for key, value in first_feature['properties'].items():
                logger.info(f"- {key}: {value}")
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise
    finally:
        # Cleanup
        if temp_dir.exists():
            import shutil
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    import asyncio
    asyncio.run(inspect_carbon_zip())