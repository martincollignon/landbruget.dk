import pandas as pd
import geopandas as gpd
import aiohttp
import xml.etree.ElementTree as ET
from shapely.geometry import Polygon, MultiPolygon
from datetime import datetime
from ..base import Source
import asyncio
import os
from dotenv import load_dotenv
from tqdm import tqdm

def clean_value(value):
    """Convert NaN values to None and leave other values unchanged"""
    import math
    if isinstance(value, float) and math.isnan(value):
        return None
    return value

class Cadastral(Source):
    """Danish Cadastral WFS parser for fetching and syncing cadastral data"""
    
    def __init__(self, config):
        """Initialize the cadastral parser with configuration and credentials"""
        super().__init__(config)
        load_dotenv()
        self.username = os.getenv('DATAFORDELER_USERNAME')
        self.password = os.getenv('DATAFORDELER_PASSWORD')
        if not self.username or not self.password:
            raise ValueError("Missing DATAFORDELER_USERNAME or DATAFORDELER_PASSWORD environment variables")
        self.page_size = 10000  # Number of records per request
        self.max_concurrent = 20  # Number of parallel requests
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
        """Parse geometry from XML to GeoJSON format"""
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
        """Parse a single feature from XML to GeoJSON format"""
        feature = {
            'type': 'Feature',
            'properties': {},
            'geometry': None
        }
        
        for element in member:
            if element.tag.endswith('geometri'):
                continue
            
            field_name = element.tag.split('}')[-1]
            if element.text and element.text.strip():
                value = element.text.strip()
                
                if value.lower() in ('true', 'false'):
                    value = value.lower() == 'true'
                elif value.isdigit():
                    value = int(value)
                elif value.replace('.', '').isdigit() and '.' in value:
                    value = float(value)
                    
                feature['properties'][field_name] = value
        
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
        """Fetch and parse a chunk of features"""
        params = self._get_params(start_index, self.page_size)
        features = []
        
        async with session.get(self.config['url'], params=params) as response:
            response.raise_for_status()
            parser = ET.XMLPullParser(['end'])
            chunk_size = 8192
            
            while True:
                chunk = await response.content.read(chunk_size)
                if not chunk:
                    break
                parser.feed(chunk)
                for event, elem in parser.read_events():
                    if elem.tag.endswith('SamletFastEjendom_Gaeldende'):
                        feature = self._parse_feature(elem, self.namespaces)
                        features.append(feature)
                        elem.clear()
        
        return features

    async def fetch(self) -> pd.DataFrame:
        """Fetch all cadastral data and return as GeoDataFrame"""
        all_features = []
        
        async with self._get_session() as session:
            # Get total count
            params = self._get_params(0, 1)
            async with session.get(self.config['url'], params=params) as response:
                response.raise_for_status()
                content = await response.text()
                root = ET.fromstring(content)
                total_features = int(root.get('numberMatched', '0'))
                if total_features == 0:
                    raise ValueError("Could not determine total number of features")
            
            print(f"Total features to fetch: {total_features:,}")
            
            # Create progress bar for overall progress
            with tqdm(total=total_features, 
                     desc="Fetching features", 
                     unit="features",
                     unit_scale=True,
                     miniters=1) as pbar:
                
                batch_size = self.page_size * self.max_concurrent
                for batch_start in range(0, total_features, batch_size):
                    remaining = total_features - batch_start
                    # Adjust concurrency for last batch
                    if remaining < batch_size:
                        current_concurrent = max(1, remaining // self.page_size)
                        print(f"\nReducing concurrency to {current_concurrent} for final {remaining:,} records")
                    else:
                        current_concurrent = self.max_concurrent
                    
                    tasks = []
                    for offset in range(0, min(batch_size, remaining), self.page_size):
                        start_idx = batch_start + offset
                        if start_idx >= total_features:
                            break
                        tasks.append(self._fetch_chunk(session, start_idx))
                        if len(tasks) >= current_concurrent:
                            break
                    
                    batch_results = await asyncio.gather(*tasks)
                    for features in batch_results:
                        all_features.extend(features)
                        pbar.update(len(features))
            
            print(f"\nConverting {len(all_features):,} features to GeoDataFrame...")
            gdf = gpd.GeoDataFrame.from_features(all_features)
            gdf.set_crs(epsg=25832, inplace=True)
            return gdf

    async def sync(self, client):
        """Sync cadastral data to Supabase database"""
        print("Starting cadastral sync...")
        print("Fetching data...")
        gdf = await self.fetch()
        
        print("\nPreparing records...")
        records = []
        with tqdm(total=len(gdf), 
                 desc="Converting records", 
                 unit="records",
                 unit_scale=True,
                 miniters=1) as pbar:
            for _, row in gdf.iterrows():
                record = {
                    'bfe_number': clean_value(row['BFEnummer']),
                    'business_event': clean_value(row['forretningshaendelse']),
                    'business_process': clean_value(row['forretningsproces']),
                    'latest_case_id': clean_value(row['senesteSagLokalId']),
                    'id_namespace': clean_value(row['id.namespace']),
                    'id_local': clean_value(row['id.lokalId']),
                    'registration_from': clean_value(row['registreringFra']),
                    'effect_from': clean_value(row['virkningFra']),
                    'authority': clean_value(row['virkningsaktoer']),
                    'is_worker_housing': clean_value(row['arbejderbolig']),
                    'is_common_lot': clean_value(row['erFaelleslod']),
                    'has_owner_apartments': clean_value(row['hovedejendomOpdeltIEjerlejligheder']),
                    'is_separated_road': clean_value(row['udskiltVej']),
                    'agricultural_notation': clean_value(row['landbrugsnotering']),
                    'geometry': row['geometry'].wkt if 'geometry' in row else None
                }
                records.append(record)
                pbar.update(1)
        
        batch_size = 2000
        total_records = len(records)
        
        print("\nUploading to database...")
        with tqdm(total=total_records, 
                 desc="Uploading records", 
                 unit="records",
                 unit_scale=True,
                 miniters=1) as pbar:
            for i in range(0, total_records, batch_size):
                batch = records[i:i + batch_size]
                response = client.table('cadastral_properties').upsert(batch).execute()
                pbar.update(len(batch))
        
        print("\nSync completed!")
        return total_records