import logging
import aiohttp
import geopandas as gpd
import asyncio
import xml.etree.ElementTree as ET
from ..base import Source
import time

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
        self.batch_size = 500
        self.max_concurrent = 10
        self.storage_batch_size = 2000
        
        self.request_semaphore = asyncio.Semaphore(self.max_concurrent)
        self.start_time = None
        self.features_processed = 0

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
            async with session.get(self.config['url'], params=params) as response:
                if response.status == 200:
                    text = await response.text()
                    root = ET.fromstring(text)
                    total = int(root.get('numberMatched', '0'))
                    logger.info(f"Total features available: {total:,}")
                    return total
                else:
                    logger.error(f"Error getting count: {response.status}")
                    return 0
        except Exception as e:
            logger.error(f"Error getting total count: {str(e)}")
            return 0

    async def _fetch_chunk(self, session, start_index):
        """Fetch a chunk of features"""
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
                async with session.get(self.config['url'], params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        features = data.get('features', [])
                        
                        if not features:
                            return None
                            
                        gdf = gpd.GeoDataFrame.from_features(features)
                        gdf = gdf.rename(columns=self.COLUMN_MAPPING)
                        
                        chunk_time = time.time() - chunk_start
                        logger.debug(f"Processed {len(features)} features in {chunk_time:.2f}s")
                        return gdf
                    else:
                        logger.error(f"Error response {response.status} at index {start_index}")
                        return None
                        
            except Exception as e:
                logger.error(f"Error fetching chunk at index {start_index}: {str(e)}")
                return None

    async def sync(self):
        """Sync agricultural fields data"""
        logger.info("Starting agricultural fields sync...")
        self.start_time = time.time()
        self.features_processed = 0
        features_batch = []
        
        async with aiohttp.ClientSession() as session:
            total_features = await self._get_total_count(session)
            if total_features == 0:
                return 0
            
            tasks = []
            for start_index in range(0, total_features, self.batch_size):
                # Manage concurrent tasks
                if len(tasks) >= self.max_concurrent:
                    completed, tasks = await asyncio.wait(
                        tasks, 
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in completed:
                        chunk = await task
                        if chunk is not None:
                            features_batch.extend(chunk.to_dict('records'))
                
                # Create new task
                task = asyncio.create_task(self._fetch_chunk(session, start_index))
                tasks.append(task)
                
                # Write to storage when batch is large enough
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
            
            # Process remaining tasks
            if tasks:
                for chunk in await asyncio.gather(*tasks):
                    if chunk is not None:
                        features_batch.extend(chunk.to_dict('records'))
            
            # Write remaining features
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

    async def fetch(self):
        return await self.sync()
