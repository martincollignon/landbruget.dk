import pandas as pd
import geopandas as gpd
import aiohttp
import xml.etree.ElementTree as ET
from shapely.geometry import Polygon, MultiPolygon
from datetime import datetime
from ..base import Source
import asyncio

class Cadastral(Source):
    """Danish Cadastral WFS parser"""
    
    def __init__(self, config):
        super().__init__(config)
        self.page_size = 10000  # Larger page size for fewer requests
        self.max_concurrent = 5  # Number of parallel requests
        self.namespaces = {
            'wfs': 'http://www.opengis.net/wfs/2.0',
            'mat': 'http://data.gov.dk/schemas/matrikel/1',
            'gml': 'http://www.opengis.net/gml/3.2'
        }

    def _get_params(self, start_index=0, max_features=None):
        """Get WFS request parameters using WFS 1.1.0"""
        params = {
            'username': 'DATAFORDELER_USERNAME_PLACEHOLDER',
            'password': 'DATAFORDELER_PASSWORD_PLACEHOLDER',
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

    def _get_session(self):
        """Create an HTTP session with connection pooling"""
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        return aiohttp.ClientSession(
            headers={'User-Agent': 'Mozilla/5.0 QGIS/33603/macOS 15.1'},
            connector=connector
        )

    async def _fetch_chunk(self, session, start_index):
        """Fetch and parse a chunk of features"""
        params = self._get_params(start_index, self.page_size)
        features = []
        
        async with session.get(self.config['url'], params=params) as response:
            response.raise_for_status()
            
            # Stream and parse the response
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
                        elem.clear()  # Free memory
        
        return features

    def _parse_feature(self, member, namespaces):
        """Parse a single feature from XML"""
        feature = {
            'type': 'Feature',
            'properties': {},
            'geometry': None
        }
        
        # Get all fields from the XML
        for element in member:
            # Skip geometry element as we'll handle it separately
            if element.tag.endswith('geometri'):
                continue
                
            # Get field name without namespace
            field_name = element.tag.split('}')[-1]
            
            # Get field value if it exists
            if element.text and element.text.strip():
                # Convert types appropriately
                value = element.text.strip()
                
                # Convert booleans
                if value.lower() in ('true', 'false'):
                    value = value.lower() == 'true'
                # Convert integers
                elif value.isdigit():
                    value = int(value)
                # Convert floats (if it has a decimal point)
                elif value.replace('.', '').isdigit() and '.' in value:
                    value = float(value)
                    
                feature['properties'][field_name] = value
        
        # Parse geometry from the geometri element
        geom_elem = member.find('mat:geometri', namespaces)
        if geom_elem is not None:
            # Look for MultiSurface or Polygon within geometri
            multi_surface = geom_elem.find('.//gml:MultiSurface', namespaces)
            if multi_surface is not None:
                feature['geometry'] = self._parse_geometry(multi_surface, namespaces)
            else:
                polygon = geom_elem.find('.//gml:Polygon', namespaces)
                if polygon is not None:
                    feature['geometry'] = self._parse_geometry(polygon, namespaces)
            
        return feature

    def _parse_geometry(self, geom, namespaces):
        """Parse geometry from XML"""
        if geom.tag.endswith('Polygon'):
            coords = []
            for pos_list in geom.findall('.//gml:posList', namespaces):
                points = [float(x) for x in pos_list.text.split()]
                # Points come in pairs
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

    async def fetch(self) -> pd.DataFrame:
        """Fetch cadastral data using parallel streaming requests"""
        all_features = []
        
        async with self._get_session() as session:
            # Get total count first using resultType=hits
            count_params = {
                'username': 'DATAFORDELER_USERNAME_PLACEHOLDER',
                'password': 'DATAFORDELER_PASSWORD_PLACEHOLDER',
                'SERVICE': 'WFS',
                'REQUEST': 'GetFeature',
                'VERSION': '1.1.0',
                'TYPENAME': 'mat:SamletFastEjendom_Gaeldende',
                'NAMESPACE': 'xmlns(mat=http://data.gov.dk/schemas/matrikel/1)',
                'resultType': 'hits'
            }
            
            async with session.get(self.config['url'], params=count_params) as response:
                response.raise_for_status()
                content = await response.text()
                root = ET.fromstring(content)
                total_features = int(root.get('numberMatched', '0'))
                
                if total_features == 0:
                    raise ValueError("Could not determine total number of features")
                
            print(f"Total features to fetch: {total_features:,}")
            
            # Process in batches to control memory usage
            batch_size = self.page_size * self.max_concurrent
            for batch_start in range(0, total_features, batch_size):
                tasks = []
                for offset in range(0, batch_size, self.page_size):
                    start_idx = batch_start + offset
                    if start_idx >= total_features:
                        break
                    tasks.append(self._fetch_chunk(session, start_idx))
                
                print(f"Fetching records {batch_start:,} to {min(batch_start + batch_size, total_features):,}...")
                batch_results = await asyncio.gather(*tasks)
                
                # Process results as they come in
                for features in batch_results:
                    all_features.extend(features)
                    print(f"Total features collected: {len(all_features):,}")
        
        print(f"\nConverting {len(all_features):,} features to GeoDataFrame...")
        gdf = gpd.GeoDataFrame.from_features(all_features)
        gdf.set_crs(epsg=25832, inplace=True)
        return gdf

    async def sync(self, client):
        """Sync cadastral data to Supabase PostGIS database"""
        print("Starting cadastral sync...")
        
        # 1. Create table if it doesn't exist (using PostGIS)
        await client.execute("""
            create extension if not exists postgis;
            
            create table if not exists cadastral (
                id bigint generated by default as identity primary key,
                forretningshaendelse text,
                seneste_sag_lokal_id text,
                forretningsproces text,
                id_namespace text,
                id_lokal_id text,
                registrering_fra timestamptz,
                virkning_fra timestamptz,
                virkningsaktoer text,
                bfe_number integer,
                arbejderbolig boolean,
                er_faelleslod boolean,
                hovedejendom_opdelt_i_ejerlejligheder boolean,
                udskilt_vej boolean,
                landbrugsnotering text,
                geometry geometry(MultiPolygon, 25832),
                created_at timestamptz default now(),
                updated_at timestamptz default now()
            );
            
            create index if not exists cadastral_bfe_number_idx on cadastral(bfe_number);
            create index if not exists cadastral_geometry_idx on cadastral using gist(geometry);
        """)
        
        # 2. Fetch data
        print("Fetching data...")
        gdf = await self.fetch()
        
        # 3. Start transaction for the sync
        async with client.transaction():
            # Create temp table with PostGIS
            print("Creating temporary table...")
            await client.execute("""
                create temporary table cadastral_temp (like cadastral excluding id, created_at);
            """)
            
            # Insert into temp table using ST_GeomFromGeoJSON for PostGIS
            print(f"Processing {len(gdf):,} records...")
            for _, row in gdf.iterrows():
                await client.execute("""
                    insert into cadastral_temp (
                        forretningshaendelse, seneste_sag_lokal_id, forretningsproces,
                        id_namespace, id_lokal_id, registrering_fra, virkning_fra,
                        virkningsaktoer, bfe_number, arbejderbolig, er_faelleslod,
                        hovedejendom_opdelt_i_ejerlejligheder, udskilt_vej,
                        landbrugsnotering, geometry
                    ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 
                        ST_SetSRID(ST_GeomFromGeoJSON($15), 25832)
                    )
                """, 
                    row['properties'].get('forretningshaendelse'),
                    row['properties'].get('senesteSagLokalId'),
                    row['properties'].get('forretningsproces'),
                    row['properties'].get('id.namespace'),
                    row['properties'].get('id.lokalId'),
                    row['properties'].get('registreringFra'),
                    row['properties'].get('virkningFra'),
                    row['properties'].get('virkningsaktoer'),
                    row['properties'].get('BFEnummer'),
                    row['properties'].get('arbejderbolig'),
                    row['properties'].get('erFaelleslod'),
                    row['properties'].get('hovedejendomOpdeltIEjerlejligheder'),
                    row['properties'].get('udskiltVej'),
                    row['properties'].get('landbrugsnotering'),
                    row.geometry.__geo_interface__
                )
            
            # Update existing records using PostGIS
            print("Updating existing records...")
            await client.execute("""
                update cadastral c
                set 
                    forretningshaendelse = t.forretningshaendelse,
                    seneste_sag_lokal_id = t.seneste_sag_lokal_id,
                    forretningsproces = t.forretningsproces,
                    id_namespace = t.id_namespace,
                    id_lokal_id = t.id_lokal_id,
                    registrering_fra = t.registrering_fra,
                    virkning_fra = t.virkning_fra,
                    virkningsaktoer = t.virkningsaktoer,
                    arbejderbolig = t.arbejderbolig,
                    er_faelleslod = t.er_faelleslod,
                    hovedejendom_opdelt_i_ejerlejligheder = t.hovedejendom_opdelt_i_ejerlejligheder,
                    udskilt_vej = t.udskilt_vej,
                    landbrugsnotering = t.landbrugsnotering,
                    geometry = t.geometry,
                    updated_at = now()
                from cadastral_temp t
                where c.bfe_number = t.bfe_number;
            """)
            
            # Get sync stats
            stats = await client.fetch_row("""
                select 
                    (select count(*) from cadastral) as total_records,
                    (select count(*) from (
                        select bfe_number from cadastral_temp
                        except
                        select bfe_number from cadastral
                    ) x) as new_records,
                    (select count(*) from (
                        select bfe_number from cadastral
                        except
                        select bfe_number from cadastral_temp
                    ) x) as removed_records
            """)
        
        print("\nSync completed!")
        print(f"Total records: {stats['total_records']:,}")
        print(f"New records: {stats['new_records']:,}")
        print(f"Removed records: {stats['removed_records']:,}")
        
        return stats['total_records']