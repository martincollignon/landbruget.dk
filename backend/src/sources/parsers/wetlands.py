from pathlib import Path
import asyncio
import xml.etree.ElementTree as ET
import logging
import aiohttp
from shapely.geometry import Polygon
import backoff
from aiohttp import ClientError, ClientTimeout
from ...base import Source
import pandas as pd
import geopandas as gpd
import os
from ..utils.geometry_validator import validate_and_transform_geometries
from shapely.ops import unary_union
import dask_geopandas
import dask.dataframe as dd
import time
import psutil

logger = logging.getLogger(__name__)

class Wetlands(Source):
    def __init__(self, config):
        super().__init__(config)
        self.batch_size = 100000
        self.max_concurrent = 5
        self.request_timeout = 300
        
        self.namespaces = {
            'wfs': 'http://www.opengis.net/wfs/2.0',
            'natur': 'http://wfs2-miljoegis.mim.dk/natur',
            'gml': 'http://www.opengis.net/gml/3.2'
        }
        
        self.request_semaphore = asyncio.Semaphore(self.max_concurrent)

    def _get_params(self, start_index=0):
        """Get WFS request parameters"""
        return {
            'SERVICE': 'WFS',
            'REQUEST': 'GetFeature',
            'VERSION': '2.0.0',
            'TYPENAMES': self.config['layer'],
            'SRSNAME': 'EPSG:25832',
            'count': str(self.batch_size),
            'startIndex': str(start_index)
        }

    def _parse_geometry(self, geom_elem):
        """Parse GML geometry into Shapely geometry"""
        try:
            coords = geom_elem.find('.//gml:posList', self.namespaces).text.split()
            coords = [(float(coords[i]), float(coords[i + 1])) 
                     for i in range(0, len(coords), 2)]
            return Polygon(coords)
        except Exception as e:
            logger.error(f"Error parsing geometry: {str(e)}")
            return None

    def _parse_feature(self, feature):
        """Parse a single feature into GeoJSON-like dictionary"""
        try:
            geom = self._parse_geometry(
                feature.find('.//gml:Polygon', self.namespaces)
            )
            
            if not geom:
                return None

            return {
                'type': 'Feature',
                'geometry': geom.__geo_interface__,
                'properties': {
                    'id': feature.get('{http://www.opengis.net/gml/3.2}id'),
                    'gridcode': int(feature.find('natur:gridcode', self.namespaces).text),
                    'toerv_pct': feature.find('natur:toerv_pct', self.namespaces).text
                }
            }
        except Exception as e:
            logger.error(f"Error parsing feature: {str(e)}")
            return None

    @backoff.on_exception(
        backoff.expo,
        (ClientError, asyncio.TimeoutError),
        max_tries=3
    )
    async def _fetch_chunk(self, session, start_index):
        """Fetch a chunk of features with retries"""
        async with self.request_semaphore:
            params = self._get_params(start_index)
            async with session.get(self.config['url'], params=params) as response:
                response.raise_for_status()
                text = await response.text()
                root = ET.fromstring(text)
                
                features = []
                for feature_elem in root.findall('.//natur:kulstof2022', self.namespaces):
                    feature = self._parse_feature(feature_elem)
                    if feature:
                        features.append(feature)
                
                return features

    async def sync(self):
        """Sync wetlands data to Cloud Storage"""
        logger.info("Starting wetlands sync...")
        self.is_sync_complete = False
        
        async with aiohttp.ClientSession() as session:
            # Get total count
            params = self._get_params(0)
            async with session.get(self.config['url'], params=params) as response:
                text = await response.text()
                root = ET.fromstring(text)
                total_features = int(root.get('numberMatched', '0'))
                logger.info(f"Total available features: {total_features:,}")
                
                # Process first batch
                features = [
                    self._parse_feature(f) 
                    for f in root.findall('.//natur:kulstof2022', self.namespaces)
                ]
                features = [f for f in features if f]
                
                if features:
                    await self.write_to_storage(features, 'wetlands')
                logger.info(f"Wrote first batch: {len(features)} features")
                
                # Process remaining batches
                total_processed = len(features)
                for start_index in range(self.batch_size, total_features, self.batch_size):
                    try:
                        chunk = await self._fetch_chunk(session, start_index)
                        if chunk:
                            self.is_sync_complete = (start_index + self.batch_size) >= total_features
                            await self.write_to_storage(chunk, 'wetlands')
                            total_processed += len(chunk)
                            logger.info(f"Progress: {total_processed:,}/{total_features:,}")
                    except Exception as e:
                        logger.error(f"Error processing batch at {start_index}: {str(e)}")
                        continue
        
        logger.info(f"Sync completed. Total processed: {total_processed:,}")
        return total_processed

    async def fetch(self):
        """Not implemented - using sync() directly"""
        raise NotImplementedError("This source uses sync() directly") 

    async def write_to_storage(self, features, dataset):
        """Write features to GeoParquet in Cloud Storage"""
        if not features:
            return
        
        try:
            # Create DataFrame from features properties
            df = pd.DataFrame([f['properties'] for f in features])
            geometries = [Polygon(f['geometry']['coordinates'][0]) for f in features]
            
            # Create GeoDataFrame with original CRS
            gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:25832")
            
            # Validate and transform geometries
            gdf = validate_and_transform_geometries(gdf, 'wetlands')
            
            # Handle working/final files
            temp_working = f"/tmp/{dataset}_working.parquet"
            working_blob = self.bucket.blob(f'raw/{dataset}/working.parquet')
            
            if working_blob.exists():
                working_blob.download_to_filename(temp_working)
                existing_gdf = gpd.read_parquet(temp_working)
                logger.info(f"Appending {len(gdf):,} features to existing {len(existing_gdf):,}")
                combined_gdf = pd.concat([existing_gdf, gdf], ignore_index=True)
            else:
                combined_gdf = gdf
                
            # Write working file
            combined_gdf.to_parquet(temp_working)
            working_blob.upload_from_filename(temp_working)
            logger.info(f"Updated working file now has {len(combined_gdf):,} features")
            
            # If sync complete, create final files
            if hasattr(self, 'is_sync_complete') and self.is_sync_complete:
                logger.info(f"Sync complete - writing final files")
                
                # Write regular final file
                final_blob = self.bucket.blob(f'raw/{dataset}/current.parquet')
                final_blob.upload_from_filename(temp_working)
                
                # Create dissolved version with parallel processing
                logger.info(f"Starting parallel dissolution of {len(combined_gdf):,} geometries...")
                logger.info(f"Memory usage before dissolution: {psutil.Process().memory_info().rss / 1024 / 1024:.2f} MB")
                start_time = time.time()
                
                # Convert to dask geodataframe (adjust npartitions based on CPU cores)
                dask_gdf = dask_geopandas.from_geopandas(combined_gdf, npartitions=4)
                
                # Perform parallel dissolution
                dissolved = dask_gdf.dissolve().compute()
                
                end_time = time.time()
                duration_minutes = (end_time - start_time) / 60
                logger.info(f"Parallel dissolution completed in {duration_minutes:.2f} minutes")
                logger.info(f"Memory usage after dissolution: {psutil.Process().memory_info().rss / 1024 / 1024:.2f} MB")
                
                dissolved_gdf = gpd.GeoDataFrame(geometry=[dissolved.geometry.iloc[0]], crs=combined_gdf.crs)
                
                # Write dissolved version
                temp_dissolved = f"/tmp/{dataset}_dissolved.parquet"
                dissolved_gdf.to_parquet(temp_dissolved)
                dissolved_blob = self.bucket.blob(f'raw/{dataset}/dissolved_current.parquet')
                dissolved_blob.upload_from_filename(temp_dissolved)
                logger.info("Dissolved version created and saved")
                
                # Cleanup
                working_blob.delete()
                os.remove(temp_dissolved)
            
            # Cleanup working file
            if os.path.exists(temp_working):
                os.remove(temp_working)
            
        except Exception as e:
            logger.error(f"Error writing to storage: {str(e)}")
            raise