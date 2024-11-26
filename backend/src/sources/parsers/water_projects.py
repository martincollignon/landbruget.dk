from pathlib import Path
import asyncio
import xml.etree.ElementTree as ET
import logging
import aiohttp
from shapely.geometry import Polygon, MultiPolygon
from datetime import datetime
import backoff
from aiohttp import ClientError, ClientTimeout
from ...base import Source, clean_value

logger = logging.getLogger(__name__)

class WaterProjects(Source):
    def __init__(self, config):
        super().__init__(config)
        self.batch_size = 1000
        self.max_concurrent = 5
        self.request_timeout = 300
        
        self.request_timeout_config = ClientTimeout(
            total=self.request_timeout,
            connect=60,
            sock_read=300
        )
        
        self.layers = [
            "N2000_projekter:Hydrologi_E",
            "N2000_projekter:Hydrologi_F",
            "Ovrige_projekter:Vandloebsrestaurering_E",
            "Ovrige_projekter:Vandloebsrestaurering_F",
            "Vandprojekter:Fosfor_E_samlet",
            "Vandprojekter:Fosfor_F_samlet",
            "Vandprojekter:Kvaelstof_E_samlet",
            "Vandprojekter:Kvaelstof_F_samlet",
            "Vandprojekter:Lavbund_E_samlet",
            "Vandprojekter:Lavbund_F_samlet",
            "Vandprojekter:Private_vaadomraader",
            "Vandprojekter:Restaurering_af_aadale_2024"
        ]
        
        self.request_semaphore = asyncio.Semaphore(self.max_concurrent)

    def _get_params(self, layer, start_index=0):
        """Get WFS request parameters"""
        return {
            'SERVICE': 'WFS',
            'REQUEST': 'GetFeature',
            'VERSION': '2.0.0',
            'TYPENAMES': layer,
            'STARTINDEX': str(start_index),
            'COUNT': str(self.batch_size),
            'SRSNAME': 'urn:ogc:def:crs:EPSG::25832'
        }

    def _parse_geometry(self, geom_elem):
        """Parse GML geometry into WKT"""
        try:
            coords_elem = geom_elem.find('.//*[local-name()="posList"]')
            if coords_elem is None:
                return None
                
            coords = coords_elem.text.split()
            coords = [(float(coords[i]), float(coords[i + 1])) 
                     for i in range(0, len(coords), 2)]
            return Polygon(coords)
        except Exception as e:
            logger.error(f"Error parsing geometry: {str(e)}")
            return None

    def _parse_feature(self, feature, layer_name):
        """Parse a single feature into a dictionary"""
        try:
            # Extract base data
            data = {
                'layer_name': layer_name,
                'geometry': self._parse_geometry(feature.find('.//*[local-name()="the_geom"]'))
            }
            
            # Extract all other fields
            for child in feature:
                field_name = child.tag.split('}')[-1].lower()
                if field_name != 'the_geom':
                    data[field_name] = clean_value(child.text)
            
            return data
        except Exception as e:
            logger.error(f"Error parsing feature: {str(e)}")
            return None

    @backoff.on_exception(
        backoff.expo,
        (ClientError, asyncio.TimeoutError),
        max_tries=3
    )
    async def _fetch_chunk(self, session, layer, start_index):
        """Fetch a chunk of features with retries"""
        async with self.request_semaphore:
            params = self._get_params(layer, start_index)
            try:
                logger.info(f"Fetching chunk for {layer} at index {start_index}")
                async with session.get(
                    self.config['url'], 
                    params=params,
                    timeout=self.request_timeout_config
                ) as response:
                    response.raise_for_status()
                    text = await response.text()
                    root = ET.fromstring(text)
                    
                    features = []
                    for feature_elem in root.findall('.//*[local-name()="member"]/*'):
                        feature = self._parse_feature(feature_elem, layer)
                        if feature and feature['geometry']:
                            features.append(feature)
                    
                    logger.info(f"Layer {layer}, chunk {start_index}: parsed {len(features)} features")
                    return features
                    
            except Exception as e:
                logger.error(f"Error fetching chunk for {layer} at index {start_index}: {str(e)}")
                raise

    async def _create_tables(self, client):
        """Create necessary database tables"""
        await client.execute("""
            CREATE TABLE IF NOT EXISTS water_projects (
                id SERIAL PRIMARY KEY,
                layer_name TEXT,
                area_ha NUMERIC,
                journalnr TEXT,
                titel TEXT,
                ansoeger TEXT,
                marknr TEXT,
                cvr TEXT,
                startaar INTEGER,
                tilsagnsaa INTEGER,
                slutaar INTEGER,
                startdato DATE,
                slutdato DATE,
                ordning TEXT,
                budget TEXT,
                indsats TEXT,
                geometry GEOMETRY(POLYGON, 25832)
            );
            
            CREATE INDEX IF NOT EXISTS water_projects_geometry_idx 
            ON water_projects USING GIST (geometry);
            
            CREATE INDEX IF NOT EXISTS water_projects_layer_idx 
            ON water_projects (layer_name);
        """)

    async def _insert_batch(self, client, features):
        """Insert a batch of features"""
        if not features:
            return 0
            
        try:
            values = []
            for f in features:
                # Use either areal_ha or imk_areal, preferring areal_ha if both exist
                area = None
                if f.get('areal_ha'):
                    area = float(f['areal_ha'])
                elif f.get('imk_areal'):
                    area = float(f['imk_areal'])

                values.append((
                    f['layer_name'],
                    area,
                    f.get('journalnr'),
                    f.get('titel'),
                    f.get('ansoeger'),
                    f.get('marknr'),
                    f.get('cvr'),
                    clean_value(f.get('startaar')),
                    clean_value(f.get('tilsagnsaa')),
                    clean_value(f.get('slutaar')),
                    datetime.strptime(f['startdato'], '%d-%m-%Y').date() if f.get('startdato') else None,
                    datetime.strptime(f['slutdato'], '%d-%m-%Y').date() if f.get('slutdato') else None,
                    f.get('ordning'),
                    f.get('budget'),
                    f.get('indsats'),
                    f['geometry'].wkt
                ))

            await client.executemany("""
                INSERT INTO water_projects (
                    layer_name, area_ha, journalnr, titel, 
                    ansoeger, marknr, cvr, startaar, tilsagnsaa, slutaar,
                    startdato, slutdato, ordning, budget, indsats, geometry
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 
                        $12, $13, $14, $15, ST_GeomFromText($16, 25832))
            """, values)
            
            return len(values)
            
        except Exception as e:
            logger.error(f"Error inserting batch: {str(e)}")
            raise

    async def sync(self, client):
        """Sync water projects data"""
        logger.info("Starting water projects sync...")
        start_time = datetime.now()
        
        await self._create_tables(client)
        
        # Clear existing data
        await client.execute("TRUNCATE TABLE water_projects")
        
        total_processed = 0
        
        async with aiohttp.ClientSession() as session:
            for layer in self.layers:
                try:
                    # Get initial batch to determine total count
                    params = self._get_params(layer, 0)
                    async with session.get(self.config['url'], params=params) as response:
                        text = await response.text()
                        root = ET.fromstring(text)
                        total_features = int(root.get('numberMatched', '0'))
                        
                        # Process first batch
                        features = [
                            self._parse_feature(f, layer) 
                            for f in root.findall('.//*[local-name()="member"]/*')
                        ]
                        features = [f for f in features if f and f['geometry']]
                        
                        if features:
                            inserted = await self._insert_batch(client, features)
                            total_processed += inserted
                        
                        # Process remaining batches
                        for start_index in range(self.batch_size, total_features, self.batch_size):
                            features = await self._fetch_chunk(session, layer, start_index)
                            if features:
                                inserted = await self._insert_batch(client, features)
                                total_processed += inserted
                                
                except Exception as e:
                    logger.error(f"Error processing layer {layer}: {str(e)}")
                    continue
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Sync completed. Total processed: {total_processed:,}")
        logger.info(f"Total runtime: {duration}")
        
        return total_processed

    async def fetch(self):
        """Not implemented - using sync() directly"""
        raise NotImplementedError("This source uses sync() directly") 