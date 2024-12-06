from pathlib import Path
import asyncio
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
import aiohttp
from shapely.geometry import Polygon, MultiPolygon
from shapely import wkt
import geopandas as gpd
from google.cloud import storage
import time
import backoff
from aiohttp import ClientError, ClientTimeout
from dotenv import load_dotenv
from tqdm import tqdm
import psutil
import pandas as pd

from ...base import Source
from ..utils.geometry_validator import validate_and_transform_geometries

logger = logging.getLogger(__name__)

def clean_value(value):
    """Clean string values"""
    if not isinstance(value, str):
        return value
    value = value.strip()
    return value if value else None

class Cadastral(Source):
    def __init__(self, config):
        super().__init__(config)
        self.field_mapping = {
            'BFEnummer': ('bfe_number', int),
            'forretningshaendelse': ('business_event', str),
            'forretningsproces': ('business_process', str),
            'senesteSagLokalId': ('latest_case_id', str),
            'id_lokalId': ('id_local', str),
            'id_namespace': ('id_namespace', str),
            'registreringFra': ('registration_from', lambda x: datetime.fromisoformat(x.replace('Z', '+00:00'))),
            'virkningFra': ('effect_from', lambda x: datetime.fromisoformat(x.replace('Z', '+00:00'))),
            'virkningsaktoer': ('authority', str),
            'arbejderbolig': ('is_worker_housing', lambda x: x.lower() == 'true'),
            'erFaelleslod': ('is_common_lot', lambda x: x.lower() == 'true'),
            'hovedejendomOpdeltIEjerlejligheder': ('has_owner_apartments', lambda x: x.lower() == 'true'),
            'udskiltVej': ('is_separated_road', lambda x: x.lower() == 'true'),
            'landbrugsnotering': ('agricultural_notation', str)
        }
        
        load_dotenv()
        self.username = os.getenv('DATAFORDELER_USERNAME')
        self.password = os.getenv('DATAFORDELER_PASSWORD')
        if not self.username or not self.password:
            raise ValueError("Missing DATAFORDELER_USERNAME or DATAFORDELER_PASSWORD")
        
        self.page_size = int(os.getenv('CADASTRAL_PAGE_SIZE', '1000'))
        self.batch_size = int(os.getenv('CADASTRAL_BATCH_SIZE', '5000'))
        self.max_concurrent = int(os.getenv('CADASTRAL_MAX_CONCURRENT', '5'))
        self.request_timeout = int(os.getenv('CADASTRAL_REQUEST_TIMEOUT', '300'))
        self.total_timeout = int(os.getenv('CADASTRAL_TOTAL_TIMEOUT', '7200'))
        self.requests_per_second = int(os.getenv('CADASTRAL_REQUESTS_PER_SECOND', '2'))
        self.last_request_time = {}
        self.request_semaphore = asyncio.Semaphore(self.max_concurrent)
        
        self.request_timeout_config = aiohttp.ClientTimeout(
            total=self.request_timeout,
            connect=60,
            sock_read=300
        )
        
        self.total_timeout_config = aiohttp.ClientTimeout(
            total=self.total_timeout,
            connect=60,
            sock_read=300
        )
        
        self.timeout = aiohttp.ClientTimeout(total=self.request_timeout)
        
        self.namespaces = {
            'wfs': 'http://www.opengis.net/wfs/2.0',
            'mat': 'http://data.gov.dk/schemas/matrikel/1',
            'gml': 'http://www.opengis.net/gml/3.2'
        }

    def _get_base_params(self):
        """Get base WFS request parameters without pagination"""
        return {
            'username': self.username,
            'password': self.password,
            'SERVICE': 'WFS',
            'REQUEST': 'GetFeature',
            'VERSION': '2.0.0',
            'TYPENAMES': 'mat:SamletFastEjendom_Gaeldende',
            'SRSNAME': 'EPSG:25832'
        }

    def _get_params(self, start_index=0):
        """Get WFS request parameters with pagination"""
        params = self._get_base_params()
        params.update({
            'startIndex': str(start_index),
            'count': str(self.page_size)
        })
        return params

    def _parse_geometry(self, geom_elem):
        """Parse GML geometry to WKT"""
        try:
            pos_lists = geom_elem.findall('.//gml:posList', self.namespaces)
            if not pos_lists:
                return None

            polygons = []
            for pos_list in pos_lists:
                if not pos_list.text:
                    continue

                coords = [float(x) for x in pos_list.text.strip().split()]
                pairs = [(coords[i], coords[i+1]) 
                        for i in range(0, len(coords), 3)]

                if len(pairs) < 4:
                    continue

                try:
                    polygon = Polygon(pairs)
                    if polygon.is_valid:
                        polygons.append(polygon)
                except Exception as e:
                    logger.warning(f"Error creating polygon: {str(e)}")
                    continue

            if not polygons:
                return None

            final_geom = MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]
            return wkt.dumps(final_geom)

        except Exception as e:
            logger.error(f"Error parsing geometry: {str(e)}")
            return None

    def _parse_feature(self, feature_elem):
        """Parse a single feature"""
        try:
            feature = {}
            
            # Parse all mapped fields
            for xml_field, (db_field, converter) in self.field_mapping.items():
                elem = feature_elem.find(f'.//mat:{xml_field}', self.namespaces)
                if elem is not None and elem.text:
                    try:
                        value = clean_value(elem.text)
                        if value is not None:
                            feature[db_field] = converter(value)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error converting field {xml_field}: {str(e)}")
                        continue

            # Parse geometry
            geom_elem = feature_elem.find('.//mat:geometri/gml:MultiSurface', self.namespaces)
            if geom_elem is not None:
                geometry_wkt = self._parse_geometry(geom_elem)
                if geometry_wkt:
                    feature['geometry'] = geometry_wkt

            return feature if feature.get('bfe_number') and feature.get('geometry') else None

        except Exception as e:
            logger.error(f"Error parsing feature: {str(e)}")
            return None

    async def _get_total_count(self, session):
        """Get total number of features from first page metadata"""
        params = self._get_base_params()
        params.update({
            'startIndex': '0',
            'count': '1'  # Just get one feature to check metadata
        })
        
        try:
            logger.info("Getting total count from first page metadata...")
            async with session.get(self.config['url'], params=params) as response:
                response.raise_for_status()
                text = await response.text()
                root = ET.fromstring(text)
                total_available = int(root.get('numberMatched', '0'))
                logger.info(f"Total available features: {total_available:,}")
                return total_available
                
        except Exception as e:
            logger.error(f"Error getting total count: {str(e)}")
            raise

    async def _wait_for_rate_limit(self):
        """Ensure we don't exceed requests_per_second"""
        worker_id = id(asyncio.current_task())
        if worker_id in self.last_request_time:
            elapsed = time.time() - self.last_request_time[worker_id]
            if elapsed < 1.0 / self.requests_per_second:
                await asyncio.sleep(1.0 / self.requests_per_second - elapsed)
        self.last_request_time[worker_id] = time.time()

    @backoff.on_exception(
        backoff.expo,
        (ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=60
    )
    async def _fetch_chunk(self, session, start_index, timeout=None):
        """Fetch a chunk of features with rate limiting and retries"""
        async with self.request_semaphore:
            await self._wait_for_rate_limit()
            
            params = self._get_params(start_index)
            
            try:
                logger.info(f"Fetching chunk at index {start_index}")
                async with session.get(
                    self.config['url'], 
                    params=params,
                    timeout=timeout or self.request_timeout_config
                ) as response:
                    if response.status == 429:  # Too Many Requests
                        retry_after = int(response.headers.get('Retry-After', 5))
                        logger.warning(f"Rate limited, waiting {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        raise ClientError("Rate limited")
                    
                    response.raise_for_status()
                    content = await response.text()
                    root = ET.fromstring(content)
                    
                    features = []
                    for feature_elem in root.findall('.//mat:SamletFastEjendom_Gaeldende', self.namespaces):
                        feature = self._parse_feature(feature_elem)
                        if feature:
                            features.append(feature)
                    
                    logger.info(f"Chunk {start_index}: parsed {len(features)} valid features")
                    return features
                    
            except Exception as e:
                logger.error(f"Error fetching chunk at index {start_index}: {str(e)}")
                raise

    async def write_to_storage(self, features, dataset):
        """Write features to GeoParquet in Cloud Storage"""
        if not features:
            return
            
        try:
            # Create DataFrame from features
            df = pd.DataFrame([{k:v for k,v in f.items() if k != 'geometry'} for f in features])
            
            # Convert WKT to shapely geometries
            geometries = [wkt.loads(f['geometry']) for f in features]
            
            # Create GeoDataFrame with original CRS
            gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:25832")
            
            # Validate and transform geometries
            gdf = validate_and_transform_geometries(gdf, 'cadastral')
            
            # Write to temporary local file
            temp_file = f"/tmp/{dataset}_current.parquet"
            gdf.to_parquet(temp_file)
            
            # Upload to Cloud Storage
            storage_client = storage.Client()
            bucket = storage_client.bucket('landbrugsdata-raw-data')
            blob = bucket.blob(f'raw/{dataset}/current.parquet')
            blob.upload_from_filename(temp_file)
            
            # Cleanup
            os.remove(temp_file)
            
            logger.info(f"Successfully wrote {len(gdf)} features to storage")
            
        except Exception as e:
            logger.error(f"Error writing to storage: {str(e)}")
            raise

    async def sync(self):
        """Sync cadastral data to Cloud Storage"""
        logger.info("Starting cadastral sync...")
        start_time = datetime.now()
        
        async with aiohttp.ClientSession(timeout=self.total_timeout_config) as session:
            # Get total count first
            total_features = await self._get_total_count(session)
            
            features = []
            total_processed = 0
            
            # Process in batches
            for start_index in range(0, total_features, self.page_size):
                try:
                    chunk = await self._fetch_chunk(session, start_index)
                    if chunk:
                        features.extend(chunk)
                        
                        # Write batch to storage when we hit batch size
                        if len(features) >= self.batch_size:
                            await self.write_to_storage(features, 'cadastral')
                            total_processed += len(features)
                            features = []
                            logger.info(f"Progress: {total_processed:,}/{total_features:,}")
                            
                except Exception as e:
                    logger.error(f"Error processing batch at {start_index}: {str(e)}")
                    continue
            
            # Write any remaining features
            if features:
                await self.write_to_storage(features, 'cadastral')
                total_processed += len(features)
            
            logger.info(f"Sync completed. Total processed: {total_processed:,}")
            return total_processed

    async def fetch(self):
        """Implement abstract method - using sync() instead"""
        logger.info("Fetch method called - using sync() instead")
        return await self.sync()