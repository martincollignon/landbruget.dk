from pathlib import Path
import asyncio
import xml.etree.ElementTree as ET
import logging
import aiohttp
from shapely.geometry import Polygon, MultiPolygon
from shapely.wkt import dumps as wkt_dumps
from datetime import datetime
import backoff
from aiohttp import ClientError, ClientTimeout
from ...base import Source, clean_value

logger = logging.getLogger(__name__)

class Wetlands(Source):
    def __init__(self, config):
        super().__init__(config)
        self.batch_size = 100000
        self.max_concurrent = 5
        self.request_timeout = 300
        self.total_timeout = 7200
        
        self.request_timeout_config = ClientTimeout(
            total=self.request_timeout,
            connect=60,
            sock_read=300
        )
        
        self.total_timeout_config = ClientTimeout(
            total=self.total_timeout,
            connect=60,
            sock_read=300
        )
        
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
        """Parse GML geometry into WKT"""
        try:
            coords = geom_elem.find('.//gml:posList', self.namespaces).text.split()
            coords = [(float(coords[i]), float(coords[i + 1])) 
                     for i in range(0, len(coords), 2)]
            return Polygon(coords)
        except Exception as e:
            logger.error(f"Error parsing geometry: {str(e)}")
            return None

    def _parse_feature(self, feature):
        """Parse a single feature into a dictionary"""
        try:
            return {
                'id': feature.get('{http://www.opengis.net/gml/3.2}id'),
                'gridcode': int(feature.find('natur:gridcode', self.namespaces).text),
                'toerv_pct': feature.find('natur:toerv_pct', self.namespaces).text,
                'geometry': self._parse_geometry(
                    feature.find('.//gml:Polygon', self.namespaces)
                )
            }
        except Exception as e:
            logger.error(f"Error parsing feature: {str(e)}")
            return None

    @backoff.on_exception(
        backoff.expo,
        (ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=60
    )
    async def _fetch_chunk(self, session, start_index):
        """Fetch a chunk of features with retries"""
        async with self.request_semaphore:
            params = self._get_params(start_index)
            try:
                logger.info(f"Fetching chunk at index {start_index}")
                async with session.get(
                    self.config['url'], 
                    params=params,
                    timeout=self.request_timeout_config
                ) as response:
                    response.raise_for_status()
                    text = await response.text()
                    root = ET.fromstring(text)
                    
                    features = []
                    for feature_elem in root.findall('.//natur:kulstof2022', self.namespaces):
                        feature = self._parse_feature(feature_elem)
                        if feature and feature['geometry']:
                            features.append(feature)
                    
                    logger.info(f"Chunk {start_index}: parsed {len(features)} valid features")
                    return features
                    
            except Exception as e:
                logger.error(f"Error fetching chunk at index {start_index}: {str(e)}")
                raise

    async def _create_tables(self, client):
        """Create necessary database tables"""
        await client.execute("""
            CREATE TABLE IF NOT EXISTS wetlands (
                id TEXT PRIMARY KEY,
                gridcode INTEGER,
                toerv_pct TEXT,
                geometry GEOMETRY(POLYGON, 25832)
            );
            
            CREATE INDEX IF NOT EXISTS wetlands_geometry_idx 
            ON wetlands USING GIST (geometry);
        """)

    async def _insert_batch(self, client, features):
        """Insert a batch of features"""
        if not features:
            return 0
            
        try:
            values = [
                (f['id'], f['gridcode'], f['toerv_pct'], f['geometry'].wkt)
                for f in features
            ]

            result = await client.executemany("""
                INSERT INTO wetlands (id, gridcode, toerv_pct, geometry)
                VALUES ($1, $2, $3, ST_GeomFromText($4, 25832))
                ON CONFLICT (id) DO UPDATE SET
                    gridcode = EXCLUDED.gridcode,
                    toerv_pct = EXCLUDED.toerv_pct,
                    geometry = EXCLUDED.geometry
            """, values)
            
            return len(values)
            
        except Exception as e:
            logger.error(f"Error inserting batch: {str(e)}")
            raise

    async def sync(self, client):
        """Sync wetlands data"""
        logger.info("Starting wetlands sync...")
        start_time = datetime.now()
        
        await self._create_tables(client)
        
        async with aiohttp.ClientSession(
            timeout=self.total_timeout_config,
            headers={'User-Agent': 'Mozilla/5.0 QGIS/33603/macOS 15.1'}
        ) as session:
            # Get total count
            params = self._get_params(0)
            async with session.get(self.config['url'], params=params) as response:
                text = await response.text()
                root = ET.fromstring(text)
                total_features = int(root.get('numberMatched', '0'))
                logger.info(f"Total available features: {total_features:,}")
                
                # Process first batch
                first_batch = [
                    self._parse_feature(f) 
                    for f in root.findall('.//natur:kulstof2022', self.namespaces)
                ]
                first_batch = [f for f in first_batch if f and f['geometry']]
                
                if first_batch:
                    await self._insert_batch(client, first_batch)
                logger.info(f"Inserted first batch: {len(first_batch)} features")
                
                # Process remaining batches
                total_processed = len(first_batch)
                for start_index in range(self.batch_size, total_features, self.batch_size):
                    try:
                        features = await self._fetch_chunk(session, start_index)
                        if features:
                            inserted = await self._insert_batch(client, features)
                            total_processed += inserted
                            logger.info(f"Progress: {total_processed:,}/{total_features:,} features")
                    except Exception as e:
                        logger.error(f"Error processing batch at {start_index}: {str(e)}")
                        continue
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Sync completed. Total processed: {total_processed:,}")
        logger.info(f"Total runtime: {duration}")
        
        return total_processed

    async def fetch(self):
        """Not implemented - using sync() directly"""
        raise NotImplementedError("This source uses sync() directly") 