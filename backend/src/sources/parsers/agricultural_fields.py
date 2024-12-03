import logging
import aiohttp
import geopandas as gpd
import asyncio
import xml.etree.ElementTree as ET
from ...base import Source
import time
import os

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
        self.batch_size = 100
        self.max_concurrent = 5
        self.storage_batch_size = 1000
        self.max_retries = 3
        
        self.timeout_config = aiohttp.ClientTimeout(
            total=1200,
            connect=60,
            sock_read=540
        )
        
        self.request_semaphore = asyncio.Semaphore(self.max_concurrent)
        self.start_time = None
        self.features_processed = 0
        self.bucket = self.storage_client.bucket(config['bucket'])
        logger.info(f"Initialized with bucket: {config['bucket']}")

    async def _get_total_count(self, session):
        """Get total number of features"""
        params = {
            'service': 'WFS',
            'version': '2.0.0',
            'request': 'GetFeature',
            'typeName': self.config['layer'],
            'resultType': 'hits'
        }
        
        try:
            logger.info(f"Fetching total count from {self.config['url']}")
            async with session.get(self.config['url'], params=params) as response:
                if response.status == 200:
                    text = await response.text()
                    root = ET.fromstring(text)
                    total = int(root.get('numberMatched', '0'))
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
            'service': 'WFS',
            'version': '2.0.0',
            'request': 'GetFeature',
            'typeName': self.config['layer'],
            'outputFormat': 'application/json',
            'startIndex': str(start_index),
            'count': str(self.batch_size)
        }
        
        async with self.request_semaphore:
            try:
                chunk_start = time.time()
                logger.debug(f"Fetching from URL: {self.config['url']} (attempt {retry_count + 1})")
                async with session.get(self.config['url'], params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        features = data.get('features', [])
                        
                        if not features:
                            logger.warning(f"No features returned at index {start_index}")
                            return None
                            
                        logger.debug(f"Creating GeoDataFrame from {len(features)} features")
                        gdf = gpd.GeoDataFrame.from_features(features)
                        gdf = gdf.rename(columns=self.COLUMN_MAPPING)
                        
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
                        
            except asyncio.TimeoutError:
                logger.error(f"Timeout at index {start_index}")
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
        features_batch = []
        
        try:
            conn = aiohttp.TCPConnector(limit=self.max_concurrent)
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
                                    features_batch.extend(chunk.to_dict('records'))
                            except Exception as e:
                                logger.error(f"Error processing task: {str(e)}")
                                continue
                    
                    await asyncio.sleep(0.1)
                    
                    task = asyncio.create_task(self._fetch_chunk(session, start_index))
                    tasks.append(task)
                    
                    if len(features_batch) >= self.storage_batch_size:
                        await self.write_to_storage(features_batch, 'agricultural_fields')
                        self.features_processed += len(features_batch)
                        elapsed = time.time() - self.start_time
                        speed = self.features_processed / elapsed
                        remaining = total_features - self.features_processed
                        eta_minutes = (remaining / speed) / 60 if speed > 0 else 0
                        
                        logger.info(
                            f"Progress: {self.features_processed:,}/{total_features:,} "
                            f"({speed:.1f} features/second, ETA: {eta_minutes:.1f} minutes)"
                        )
                        features_batch = []
                
                if tasks:
                    done, _ = await asyncio.wait(tasks)
                    for task in done:
                        try:
                            chunk = await task
                            if chunk is not None:
                                features_batch.extend(chunk.to_dict('records'))
                        except Exception as e:
                            logger.error(f"Error processing remaining task: {str(e)}")
                            continue
                
                if features_batch:
                    await self.write_to_storage(features_batch, 'agricultural_fields')
                    self.features_processed += len(features_batch)
                
                total_time = time.time() - self.start_time
                final_speed = self.features_processed / total_time
                logger.info(
                    f"Sync completed. Processed {self.features_processed:,} features "
                    f"in {total_time:.1f}s ({final_speed:.1f} features/second)"
                )
                return self.features_processed
                
        except Exception as e:
            logger.error(f"Error in sync: {str(e)}", exc_info=True)
            return self.features_processed

    async def fetch(self):
        return await self.sync()

    async def write_to_storage(self, features, dataset):
        """Write features to GeoParquet in Cloud Storage"""
        if not features:
            return
        
        try:
            gdf = gpd.GeoDataFrame(features)
            
            temp_file = f"/tmp/{dataset}_current.parquet"
            gdf.to_parquet(temp_file)
            
            blob = self.bucket.blob(f'raw/{dataset}/current.parquet')
            blob.upload_from_filename(temp_file)
            
            os.remove(temp_file)
            
            logger.info(f"Successfully wrote {len(gdf)} features to storage")
            
        except Exception as e:
            logger.error(f"Error writing to storage: {str(e)}")
            raise
