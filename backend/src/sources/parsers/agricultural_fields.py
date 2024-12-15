import logging
import aiohttp
import geopandas as gpd
import asyncio
import xml.etree.ElementTree as ET
from ...base import Source
import time
import os
import ssl
from ..utils.geometry_validator import validate_and_transform_geometries
import pandas as pd

logger = logging.getLogger(__name__)

class AgriculturalFields(Source):
    """Danish Agricultural Fields WFS parser"""
    
    COLUMN_MAPPING = {
        'Marknr': 'field_id',
        'IMK_areal': 'area_ha',
        'Journalnr': 'journal_number',
        'CVR': 'cvr_number',
        'Afgkode': 'crop_code',
        'Afgroede': 'crop_type',
        'GB': 'organic_farming',
        'GBanmeldt': 'reported_area_ha',
        'Markblok': 'block_id'
    }
    
    def __init__(self, config):
        super().__init__(config)
        self.batch_size = 2000
        self.max_concurrent = 5
        self.storage_batch_size = 10000
        self.max_retries = 3
        
        self.timeout_config = aiohttp.ClientTimeout(
            total=1200,
            connect=60,
            sock_read=540
        )
        
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.ssl_context.options |= 0x4
        
        self.request_semaphore = asyncio.Semaphore(self.max_concurrent)
        self.start_time = None
        self.features_processed = 0
        self.bucket = self.storage_client.bucket(config['bucket'])
        logger.info(f"Initialized with batch_size={self.batch_size}, "
                   f"max_concurrent={self.max_concurrent}, "
                   f"storage_batch_size={self.storage_batch_size}")

    async def _get_total_count(self, session):
        """Get total number of features"""
        params = {
            'f': 'json',
            'where': '1=1',
            'returnCountOnly': 'true'
        }
        
        try:
            url = self.config['url']
            logger.info(f"Fetching total count from {url}")
            async with session.get(url, params=params, ssl=self.ssl_context) as response:
                if response.status == 200:
                    data = await response.json()
                    total = data.get('count', 0)
                    logger.info(f"Total features available: {total:,}")
                    return total
                else:
                    logger.error(f"Error getting count: {response.status}")
                    response_text = await response.text()
                    logger.error(f"Response: {response_text[:500]}...")
                    return 0
        except Exception as e:
            logger.error(f"Error getting total count: {str(e)}", exc_info=True)
            return 0

    async def _fetch_chunk(self, session, start_index, retry_count=0):
        """Fetch a chunk of features with retry logic"""
        params = {
            'f': 'json',
            'where': '1=1',
            'returnGeometry': 'true',
            'outFields': '*',
            'resultOffset': str(start_index),
            'resultRecordCount': str(self.batch_size)
        }
        
        async with self.request_semaphore:
            try:
                chunk_start = time.time()
                url = self.config['url']
                logger.debug(f"Fetching from URL: {url} (attempt {retry_count + 1})")
                async with session.get(url, params=params, ssl=self.ssl_context) as response:
                    if response.status == 200:
                        data = await response.json()
                        features = data.get('features', [])
                        
                        if not features:
                            logger.warning(f"No features returned at index {start_index}")
                            return None
                            
                        logger.debug(f"Creating GeoDataFrame from {len(features)} features")
                        
                        # Convert ArcGIS REST API features to GeoJSON format
                        geojson_features = []
                        for feature in features:
                            geojson_feature = {
                                'type': 'Feature',
                                'properties': feature['attributes'],
                                'geometry': {
                                    'type': 'Polygon',
                                    'coordinates': feature['geometry']['rings']
                                }
                            }
                            geojson_features.append(geojson_feature)
                        
                        gdf = gpd.GeoDataFrame.from_features(geojson_features)
                        gdf = gdf.rename(columns=self.COLUMN_MAPPING)
                        
                        # Set the CRS to EPSG:25832 (the coordinate system used by the API)
                        gdf.set_crs(epsg=25832, inplace=True)
                        
                        chunk_time = time.time() - chunk_start
                        logger.debug(f"Processed {len(features)} features in {chunk_time:.2f}s")
                        return gdf
                    else:
                        logger.error(f"Error response {response.status} at index {start_index}")
                        response_text = await response.text()
                        logger.error(f"Response: {response_text[:500]}...")
                        
                        if response.status >= 500 and retry_count < self.max_retries:
                            await asyncio.sleep(2 ** retry_count)
                            return await self._fetch_chunk(session, start_index, retry_count + 1)
                        return None
                        
            except ssl.SSLError as e:
                logger.error(f"SSL Error at index {start_index}: {str(e)}")
                if retry_count < self.max_retries:
                    await asyncio.sleep(2 ** retry_count)
                    return await self._fetch_chunk(session, start_index, retry_count + 1)
                return None
            except Exception as e:
                logger.error(f"Error fetching chunk at index {start_index}: {str(e)}")
                if retry_count < self.max_retries:
                    await asyncio.sleep(2 ** retry_count)
                    return await self._fetch_chunk(session, start_index, retry_count + 1)
                return None

    async def sync(self):
        """Sync agricultural fields data"""
        logger.info("Starting agricultural fields sync...")
        self.start_time = time.time()
        self.features_processed = 0
        self.is_sync_complete = False
        
        try:
            conn = aiohttp.TCPConnector(limit=self.max_concurrent, ssl=self.ssl_context)
            async with aiohttp.ClientSession(timeout=self.timeout_config, connector=conn) as session:
                total_features = await self._get_total_count(session)
                logger.info(f"Found {total_features:,} total features")
                
                features_batch = []
                
                for start_index in range(0, total_features, self.batch_size):
                    chunk = await self._fetch_chunk(session, start_index)
                    if chunk is not None:
                        features_batch.extend(chunk.to_dict('records'))
                        self.features_processed += len(chunk)
                        
                        # Write batch if it's large enough or it's the last batch
                        is_last_batch = (start_index + self.batch_size) >= total_features
                        if len(features_batch) >= self.storage_batch_size or is_last_batch:
                            logger.info(f"Writing batch of {len(features_batch):,} features (is_last_batch: {is_last_batch})")
                            self.is_sync_complete = is_last_batch  # Only set True for last batch
                            await self.write_to_storage(features_batch, 'agricultural_fields')
                            features_batch = []  # Clear batch after writing
                            
                            elapsed = time.time() - self.start_time
                            speed = self.features_processed / elapsed
                            remaining = total_features - self.features_processed
                            eta_minutes = (remaining / speed) / 60 if speed > 0 else 0
                            
                            logger.info(
                                f"Progress: {self.features_processed:,}/{total_features:,} "
                                f"({speed:.1f} features/second, ETA: {eta_minutes:.1f} minutes)"
                            )
                
                logger.info(f"Sync completed. Total processed: {self.features_processed:,}")
                return self.features_processed
                
        except Exception as e:
            logger.error(f"Error in sync: {str(e)}")
            raise

    async def fetch(self):
        return await self.sync()

    async def write_to_storage(self, features, dataset):
        """Write features to GeoParquet in Cloud Storage"""
        if not features:
            return
        
        try:
            # Create GeoDataFrame from ArcGIS features
            gdf = gpd.GeoDataFrame(features, crs="EPSG:25832")
            gdf.columns = [col.replace('.', '_').replace('(', '_').replace(')', '_') for col in gdf.columns]
            
            # Validate and transform geometries
            gdf = validate_and_transform_geometries(gdf, dataset)
            
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
            
            # If sync complete, create final file
            if self.is_sync_complete:
                logger.info(f"Sync complete - writing final file with {len(combined_gdf):,} features")
                final_blob = self.bucket.blob(f'raw/{dataset}/current.parquet')
                final_blob.upload_from_filename(temp_working)
                working_blob.delete()
            
            # Cleanup
            os.remove(temp_working)
            
        except Exception as e:
            logger.error(f"Error writing to storage: {str(e)}")
            raise
