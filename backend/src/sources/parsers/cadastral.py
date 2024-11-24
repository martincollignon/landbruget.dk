from pathlib import Path
import sys
import asyncio
import os
import xml.etree.ElementTree as ET
from tqdm import tqdm
import geopandas as gpd
import aiohttp
import pandas as pd
from dotenv import load_dotenv
from shapely.geometry import Polygon, MultiPolygon
import asyncpg
import logging
from collections import deque
from asyncio import Queue
from datetime import datetime

from ...base import Source, clean_value

# At the start of the file, set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Cadastral(Source):
    def __init__(self, config):
        super().__init__(config)
        load_dotenv()
        self.username = os.getenv('DATAFORDELER_USERNAME')
        self.password = os.getenv('DATAFORDELER_PASSWORD')
        if not self.username or not self.password:
            raise ValueError("Missing DATAFORDELER_USERNAME or DATAFORDELER_PASSWORD environment variables")
        self.page_size = 10000
        self.max_concurrent = 10  # Reduced for Cloud Run
        self.batch_size = 1000  # DB batch size
        self.namespaces = {
            'wfs': 'http://www.opengis.net/wfs/2.0',
            'mat': 'http://data.gov.dk/schemas/matrikel/1',
            'gml': 'http://www.opengis.net/gml/3.2'
        }

    def _get_session(self):
        """Create an HTTP session with connection pooling"""
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        return aiohttp.ClientSession(
            headers={'User-Agent': 'Mozilla/5.0 QGIS/33603/macOS 15.1'},
            connector=connector
        )

    def _get_params(self, start_index=0, max_features=None):
        """Get WFS request parameters"""
        params = {
            'username': self.username,
            'password': self.password,
            'SERVICE': 'WFS',
            'REQUEST': 'GetFeature',
            'VERSION': '1.1.0',
            'TYPENAME': 'mat:SamletFastEjendom_Gaeldende',
            'SRSNAME': 'EPSG:25832',
            'startIndex': str(start_index),
            'NAMESPACE': 'xmlns(mat=http://data.gov.dk/schemas/matrikel/1)'
        }
        if max_features:
            params['maxFeatures'] = str(max_features)
        return params

    def _parse_geometry(self, geom, namespaces):
        """Parse geometry from XML"""
        if geom.tag.endswith('Polygon'):
            coords = []
            for pos_list in geom.findall('.//gml:posList', namespaces):
                points = [float(x) for x in pos_list.text.split()]
                coords.append([[points[i], points[i+1]] for i in range(0, len(points), 2)])
            return {'type': 'Polygon', 'coordinates': coords}
            
        elif geom.tag.endswith('MultiSurface'):
            coords = []
            for polygon in geom.findall('.//gml:Polygon', namespaces):
                poly_coords = []
                for pos_list in polygon.findall('.//gml:posList', namespaces):
                    points = [float(x) for x in pos_list.text.split()]
                    poly_coords.append([[points[i], points[i+1]] for i in range(0, len(points), 2)])
                coords.append(poly_coords)
            return {'type': 'MultiPolygon', 'coordinates': coords}
            
        return None

    def _parse_feature(self, member, namespaces):
        """Parse a single feature from XML"""
        feature = {
            'type': 'Feature',
            'properties': {},
            'geometry': None
        }
        
        # Get all fields from the XML
        for element in member:
            if element.tag.endswith('geometri'):
                continue
                
            field_name = element.tag.split('}')[-1]
            
            if element.text and element.text.strip():
                value = element.text.strip()
                
                # Special handling for different field types
                if field_name == 'BFEnummer' and value.isdigit():
                    value = int(value)  # BFEnummer should be integer
                elif field_name == 'senesteSagLokalId':
                    value = str(value)  # Ensure latest_case_id stays as string
                elif value.lower() in ('true', 'false'):
                    value = value.lower() == 'true'
                elif value.isdigit():
                    value = int(value)
                elif value.replace('.', '').isdigit() and '.' in value:
                    value = float(value)
                    
                feature['properties'][field_name] = value
        
        # Parse geometry
        geom_elem = member.find('mat:geometri', namespaces)
        if geom_elem is not None:
            multi_surface = geom_elem.find('.//gml:MultiSurface', namespaces)
            if multi_surface is not None:
                feature['geometry'] = self._parse_geometry(multi_surface, namespaces)
            else:
                polygon = geom_elem.find('.//gml:Polygon', namespaces)
                if polygon is not None:
                    feature['geometry'] = self._parse_geometry(polygon, namespaces)
            
        return feature

    async def _fetch_chunk(self, session, start_index):
        """Fetch and parse a chunk of features using streaming"""
        params = self._get_params(start_index, self.page_size)
        features = []
        
        logger.info(f"Fetching chunk starting at index {start_index}")
        async with session.get(self.config['url'], params=params) as response:
            response.raise_for_status()
            
            parser = ET.XMLPullParser(['end'])
            chunk_size = 8192  # Read in 8KB chunks
            
            while True:
                chunk = await response.content.read(chunk_size)
                if not chunk:
                    break
                parser.feed(chunk)
                for event, elem in parser.read_events():
                    if elem.tag.endswith('SamletFastEjendom_Gaeldende'):
                        feature = self._parse_feature(elem, self.namespaces)
                        features.append(feature)
                        elem.clear()  # Clear element to free memory
        
        logger.info(f"Processed {len(features)} features from chunk {start_index}")
        return features

    async def _fetch_chunk_with_retry(self, session, start_index, max_retries=3, initial_delay=1):
        """Fetch chunk with exponential backoff retry"""
        delay = initial_delay
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                return await self._fetch_chunk(session, start_index)
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    logger.warning(f"Attempt {attempt + 1} failed for chunk {start_index}: {str(e)}. Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    logger.error(f"All {max_retries} attempts failed for chunk {start_index}")
        
        raise last_exception
    
    async def fetch(self) -> pd.DataFrame:
        """Fetch cadastral data using parallel streaming requests"""
        logger.info("Starting fetch process...")
        
        all_features = []
        
        async with self._get_session() as session:
            # Get total count
            logger.info("Getting total feature count...")
            count_params = self._get_params()
            count_params['resultType'] = 'hits'
            
            async with session.get(self.config['url'], params=count_params) as response:
                response.raise_for_status()
                content = await response.text()
                root = ET.fromstring(content)
                total_features = int(root.get('numberMatched', '0'))
                
                if total_features == 0:
                    raise ValueError("Could not determine total number of features")
                
            logger.info(f"Found {total_features:,} total features")
            
            # Process in batches
            batch_size = self.page_size * self.max_concurrent
            for batch_start in range(0, total_features, batch_size):
                logger.info(f"Processing batch starting at {batch_start:,}")
                tasks = []
                for offset in range(0, batch_size, self.page_size):
                    start_idx = batch_start + offset
                    if start_idx >= total_features:
                        break
                    tasks.append(self._fetch_chunk_with_retry(session, start_idx))
                
                logger.info(f"Fetching records {batch_start:,} to {min(batch_start + batch_size, total_features):,}...")
                batch_results = await asyncio.gather(*tasks)
                
                for features in batch_results:
                    all_features.extend(features)
                    logger.info(f"Total features collected: {len(all_features):,}")
        
        logger.info("Fetch process completed")
        print(f"\nConverting {len(all_features):,} features to GeoDataFrame...")
        gdf = gpd.GeoDataFrame.from_features(all_features)
        gdf.set_crs(epsg=25832, inplace=True)
        return gdf

    async def sync(self, client):
        """Parallel sync with controlled memory usage"""
        logger.info("Starting cadastral sync...")
        
        # Create database tables
        await self._create_tables(client)
        
        # Create queues
        feature_queue = Queue(maxsize=self.max_concurrent * 2)
        db_queue = Queue(maxsize=self.batch_size * 2)
        
        # Control flags
        fetch_complete = asyncio.Event()
        processing_complete = asyncio.Event()
        
        async def fetch_worker():
            try:
                async with self._get_session() as session:
                    total_features = await self._get_total_count(session)
                    logger.info(f"Found {total_features:,} total features")
                    
                    batch_size = self.page_size * self.max_concurrent
                    for batch_start in range(0, total_features, batch_size):
                        tasks = []
                        for offset in range(0, min(batch_size, total_features - batch_start), self.page_size):
                            start_idx = batch_start + offset
                            tasks.append(self._fetch_chunk_with_retry(session, start_idx))
                        
                        batch_results = await asyncio.gather(*tasks)
                        for features in batch_results:
                            await feature_queue.put(features)
                            
                fetch_complete.set()
            except Exception as e:
                logger.error(f"Fetch worker error: {str(e)}")
                raise

        async def process_worker():
            try:
                features_batch = []
                records_processed = 0
                
                while True:
                    if feature_queue.empty() and fetch_complete.is_set():
                        if features_batch:
                            await self._process_features_batch(features_batch, db_queue)
                        break
                    
                    try:
                        features = await asyncio.wait_for(feature_queue.get(), timeout=1.0)
                        features_batch.extend(features)
                        records_processed += len(features)
                        
                        if len(features_batch) >= self.batch_size:
                            await self._process_features_batch(features_batch, db_queue)
                            features_batch = []
                            logger.info(f"Processed {records_processed:,} records")
                            
                        feature_queue.task_done()
                    except asyncio.TimeoutError:
                        continue
                
                processing_complete.set()
            except Exception as e:
                logger.error(f"Process worker error: {str(e)}")
                raise

        async def db_worker():
            try:
                total_synced = 0
                
                while True:
                    if db_queue.empty() and processing_complete.is_set():
                        break
                    
                    try:
                        records = await asyncio.wait_for(db_queue.get(), timeout=1.0)
                        async with client.transaction():
                            await self._batch_insert(client, records)
                        total_synced += len(records)
                        logger.info(f"Synced {total_synced:,} records to database")
                        db_queue.task_done()
                    except asyncio.TimeoutError:
                        continue
                
                return total_synced
            except Exception as e:
                logger.error(f"DB worker error: {str(e)}")
                raise

        # Run workers
        workers = await asyncio.gather(
            fetch_worker(),
            process_worker(),
            db_worker(),
            return_exceptions=True
        )

        # Check for exceptions
        for result in workers:
            if isinstance(result, Exception):
                raise result

        return workers[-1]  # Return total synced records

    async def _process_features_batch(self, features, db_queue):
        """Process a batch of features and queue for DB insertion"""
        gdf = gpd.GeoDataFrame.from_features(features)
        records = self._prepare_records(gdf)
        await db_queue.put(records)

    def _prepare_records(self, gdf):
        """Convert GeoDataFrame rows to database records"""
        def parse_datetime(dt_str):
            if not dt_str:
                return None
            try:
                return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                logger.warning(f"Failed to parse datetime: {dt_str}")
                return None

        def clean_field(value):
            """Clean field value, handling NaN"""
            if pd.isna(value):  # This handles both np.nan and pd.NA
                return None
            return clean_value(value)

        records = []
        for _, row in gdf.iterrows():
            record = {
                'bfe_number': clean_field(row['BFEnummer']),
                'business_event': clean_field(row['forretningshaendelse']),
                'business_process': clean_field(row['forretningsproces']),
                'latest_case_id': clean_field(row['senesteSagLokalId']),
                'id_namespace': clean_field(row['id.namespace']),
                'id_local': str(clean_field(row['id.lokalId'])),
                'registration_from': parse_datetime(clean_field(row['registreringFra'])),
                'effect_from': parse_datetime(clean_field(row['virkningFra'])),
                'authority': clean_field(row['virkningsaktoer']),
                'is_worker_housing': clean_field(row['arbejderbolig']),
                'is_common_lot': clean_field(row['erFaelleslod']),
                'has_owner_apartments': clean_field(row['hovedejendomOpdeltIEjerlejligheder']),
                'is_separated_road': clean_field(row['udskiltVej']),
                'agricultural_notation': clean_field(row['landbrugsnotering']),
                'geometry': row['geometry'].wkt if 'geometry' in row else None
            }
            records.append(record)
        return records

    async def _create_tables(self, client):
        """Create necessary database tables"""
        await client.execute("""
            CREATE TABLE IF NOT EXISTS cadastral_properties (
                bfe_number INTEGER PRIMARY KEY,
                business_event TEXT,
                business_process TEXT,
                latest_case_id TEXT,
                id_namespace TEXT,
                id_local TEXT,
                registration_from TIMESTAMP WITH TIME ZONE,
                effect_from TIMESTAMP WITH TIME ZONE,
                authority TEXT,
                is_worker_housing BOOLEAN,
                is_common_lot BOOLEAN,
                has_owner_apartments BOOLEAN,
                is_separated_road BOOLEAN,
                agricultural_notation TEXT,
                geometry GEOMETRY(MULTIPOLYGON, 25832)
            );
            
            CREATE INDEX IF NOT EXISTS cadastral_properties_geometry_idx 
            ON cadastral_properties USING GIST (geometry);
        """)

    async def _get_total_count(self, session):
        """Get total feature count"""
        count_params = self._get_params()
        count_params['resultType'] = 'hits'
        
        async with session.get(self.config['url'], params=count_params) as response:
            response.raise_for_status()
            content = await response.text()
            root = ET.fromstring(content)
            return int(root.get('numberMatched', '0'))

    async def _batch_insert(self, client, records):
        """Insert records in batches"""
        query = """
            INSERT INTO cadastral_properties (
                bfe_number, business_event, business_process, latest_case_id,
                id_namespace, id_local, registration_from, effect_from,
                authority, is_worker_housing, is_common_lot, has_owner_apartments,
                is_separated_road, agricultural_notation, geometry
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, ST_GeomFromText($15, 25832))
            ON CONFLICT (bfe_number) DO UPDATE SET
                business_event = EXCLUDED.business_event,
                business_process = EXCLUDED.business_process,
                latest_case_id = EXCLUDED.latest_case_id,
                registration_from = EXCLUDED.registration_from,
                effect_from = EXCLUDED.effect_from,
                authority = EXCLUDED.authority,
                is_worker_housing = EXCLUDED.is_worker_housing,
                is_common_lot = EXCLUDED.is_common_lot,
                has_owner_apartments = EXCLUDED.has_owner_apartments,
                is_separated_road = EXCLUDED.is_separated_road,
                agricultural_notation = EXCLUDED.agricultural_notation,
                geometry = EXCLUDED.geometry
        """
        
        await client.executemany(query, [
            (
                r['bfe_number'], r['business_event'], r['business_process'],
                r['latest_case_id'], r['id_namespace'], r['id_local'],
                r['registration_from'], r['effect_from'], r['authority'],
                r['is_worker_housing'], r['is_common_lot'], r['has_owner_apartments'],
                r['is_separated_road'], r['agricultural_notation'], r['geometry']
            ) for r in records
        ])
