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
        self.is_sync_complete = False  # Initialize flag
        logger.info("Starting agricultural fields sync...")
        self.start_time = time.time()
        self.features_processed = 0
        all_features = []
        
        try:
            conn = aiohttp.TCPConnector(limit=self.max_concurrent, ssl=self.ssl_context)
            async with aiohttp.ClientSession(timeout=self.timeout_config, connector=conn) as session:
                total_features = await self._get_total_count(session)
                if total_features == 0:
                    logger.error("No features found to sync")
                    return 0
                
                tasks = []
                for start_index in range(0, total_features, self.batch_size):
                    if len(tasks) >= self.max_concurrent:
                        done, pending = await asyncio.wait(
                            tasks, 
                            return_when=asyncio.FIRST_COMPLETED
                        )
                        tasks = list(pending)
                        for task in done:
                            try:
                                chunk = await task
                                if chunk is not None:
                                    all_features.extend(chunk.to_dict('records'))
                                    self.features_processed += len(chunk)
                            except Exception as e:
                                logger.error(f"Error processing task: {str(e)}")
                                continue
                    
                    await asyncio.sleep(0.1)
                    
                    task = asyncio.create_task(self._fetch_chunk(session, start_index))
                    tasks.append(task)
                    
                    if len(all_features) >= self.storage_batch_size:
                        await self.write_to_storage(all_features, 'agricultural_fields')
                        self.features_processed += len(all_features)
                        elapsed = time.time() - self.start_time
                        speed = self.features_processed / elapsed
                        remaining = total_features - self.features_processed
                        eta_minutes = (remaining / speed) / 60 if speed > 0 else 0
                        
                        logger.info(
                            f"Progress: {self.features_processed:,}/{total_features:,} "
                            f"({speed:.1f} features/second, ETA: {eta_minutes:.1f} minutes)"
                        )
                        all_features = []
                
                if tasks:
                    done, _ = await asyncio.wait(tasks)
                    for task in done:
                        try:
                            chunk = await task
                            if chunk is not None:
                                all_features.extend(chunk.to_dict('records'))
                        except Exception as e:
                            logger.error(f"Error processing remaining task: {str(e)}")
                            continue
                
                if all_features:
                    await self.write_to_storage(all_features, 'agricultural_fields')
                    self.features_processed += len(all_features)
                
                total_time = time.time() - self.start_time
                final_speed = self.features_processed / total_time
                logger.info(
                    f"Sync completed. Processed {self.features_processed:,} features "
                    f"in {total_time:.1f}s ({final_speed:.1f} features/second)"
                )
                
                # Set completion flag before final write
                self.is_sync_complete = True
                if all_features:
                    await self.write_to_storage(all_features, 'agricultural_fields')
                
                return self.features_processed
                
        except Exception as e:
            self.is_sync_complete = False  # Reset on error
            logger.error(f"Error in sync: {str(e)}", exc_info=True)
            return self.features_processed

    async def fetch(self):
        return await self.sync()

    async def write_to_storage(self, features, dataset):
        """Write features to GeoParquet in Cloud Storage"""
        if not features:
            return
        
        try:
            # Create GeoDataFrame
            gdf = gpd.GeoDataFrame(features, crs="EPSG:25832")
            
            # Clean up column names - replace dots and parentheses with underscores
            gdf.columns = [col.replace('.', '_').replace('(', '_').replace(')', '_') for col in gdf.columns]
            
            # Validate and transform geometries
            gdf = validate_and_transform_geometries(gdf, 'agricultural_fields')
            
            # Write to temporary local file
            temp_file = f"/tmp/{dataset}_current.parquet"
            gdf.to_parquet(temp_file)
            
            # Upload to Cloud Storage
            blob = self.bucket.blob(f'raw/{dataset}/current.parquet')
            blob.upload_from_filename(temp_file)
            
            # Cleanup
            os.remove(temp_file)
            
            logger.info(f"Successfully wrote {len(gdf)} features to storage")
            
        except Exception as e:
            logger.error(f"Error writing to storage: {str(e)}")
            raise
