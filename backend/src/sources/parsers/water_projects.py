from pathlib import Path
import asyncio
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
import aiohttp
from shapely.geometry import Polygon, MultiPolygon
from shapely import wkt
from shapely.ops import unary_union
import geopandas as gpd
import pandas as pd
from google.cloud import storage
import time
import backoff
from aiohttp import ClientError, ClientTimeout
from dotenv import load_dotenv
from tqdm import tqdm
import ssl

from ...base import Source
from ..utils.geometry_validator import validate_and_transform_geometries

logger = logging.getLogger(__name__)

def clean_value(value):
    """Clean string values"""
    if not isinstance(value, str):
        return value
    value = value.strip()
    return value if value else None

class WaterProjects(Source):
    def __init__(self, config):
        super().__init__(config)
        self.batch_size = 100
        self.max_concurrent = 3
        self.request_timeout = 300
        self.storage_batch_size = 5000  # Size for storage batches
        
        self.request_timeout_config = ClientTimeout(
            total=self.request_timeout,
            connect=60,
            sock_read=300
        )
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 QGIS/33603/macOS 15.1'
        }
        
        self.layers = [
            "N2000_projekter:Hydrologi_E",
            #"N2000_projekter:Hydrologi_F",
            "Ovrige_projekter:Vandloebsrestaurering_E",
            "Ovrige_projekter:Vandloebsrestaurering_F",
            "Vandprojekter:Fosfor_E_samlet",
            "Vandprojekter:Fosfor_F_samlet",
            "Vandprojekter:Kvaelstof_E_samlet",
            "Vandprojekter:Kvaelstof_F_samlet",
            "Vandprojekter:Lavbund_E_samlet",
            "Vandprojekter:Lavbund_F_samlet",
            "Vandprojekter:Private_vaadomraader",
            "Vandprojekter:Restaurering_af_aadale_2024",
            "vandprojekter:kla_projektforslag",
            "vandprojekter:kla_projektomraader",
            "Klima_lavbund_demarkation___offentlige_projekter:0"
        ]
        
        self.request_semaphore = asyncio.Semaphore(self.max_concurrent)
        
        self.url_mapping = {
            'vandprojekter:kla_projektforslag': 'https://wfs2-miljoegis.mim.dk/vandprojekter/wfs',
            'vandprojekter:kla_projektomraader': 'https://wfs2-miljoegis.mim.dk/vandprojekter/wfs',
            'Klima_lavbund_demarkation___offentlige_projekter:0': 'https://gis.nst.dk/server/rest/services/autonom/Klima_lavbund_demarkation___offentlige_projekter/FeatureServer'
        }
        
        self.service_types = {
            'Klima_lavbund_demarkation___offentlige_projekter:0': 'arcgis'
        }

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
        """Parse GML geometry into WKT and calculate area"""
        try:
            gml_ns = '{http://www.opengis.net/gml/3.2}'
            
            multi_surface = geom_elem.find(f'.//{gml_ns}MultiSurface')
            if multi_surface is None:
                logger.error("No MultiSurface element found")
                return None
            
            polygons = []
            for surface_member in multi_surface.findall(f'.//{gml_ns}surfaceMember'):
                polygon = surface_member.find(f'.//{gml_ns}Polygon')
                if polygon is None:
                    continue
                    
                pos_list = polygon.find(f'.//{gml_ns}posList')
                if pos_list is None or not pos_list.text:
                    continue
                    
                try:
                    coords = [float(x) for x in pos_list.text.strip().split()]
                    coords = [(coords[i], coords[i+1]) for i in range(0, len(coords), 2)]
                    if len(coords) >= 4:  # Ensure we have enough coordinates
                        polygons.append(Polygon(coords))
                except Exception as e:
                    logger.error(f"Failed to parse coordinates: {str(e)}")
                    continue
            
            if not polygons:
                return None
            
            geom = MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]
            area_ha = geom.area / 10000  # Convert square meters to hectares
            
            return {
                'wkt': geom.wkt,
                'area_ha': area_ha
            }
            
        except Exception as e:
            logger.error(f"Error parsing geometry: {str(e)}")
            return None

    def _parse_feature(self, feature, layer_name):
        """Parse a single feature into a dictionary"""
        try:
            namespace = feature.tag.split('}')[0].strip('{')
            
            geom_elem = feature.find(f'{{%s}}the_geom' % namespace) or feature.find(f'{{%s}}wkb_geometry' % namespace)
            if geom_elem is None:
                logger.warning(f"No geometry found in feature for layer {layer_name}")
                return None

            geometry_data = self._parse_geometry(geom_elem)
            if geometry_data is None:
                logger.warning(f"Failed to parse geometry for layer {layer_name}")
                return None

            data = {
                'layer_name': layer_name,
                'geometry': geometry_data['wkt'],
                'area_ha': geometry_data['area_ha']
            }
            
            for elem in feature:
                if not elem.tag.endswith(('the_geom', 'wkb_geometry')):
                    key = elem.tag.split('}')[-1].lower()
                    if elem.text:
                        value = clean_value(elem.text)
                        if value is not None:
                            # Convert specific fields
                            try:
                                if key in ['area', 'budget']:
                                    value = float(''.join(c for c in value if c.isdigit() or c == '.'))
                                elif key in ['startaar', 'tilsagnsaa', 'slutaar']:
                                    value = int(value)
                                elif key in ['startdato', 'slutdato']:
                                    value = pd.to_datetime(value, dayfirst=True)
                            except (ValueError, TypeError):
                                logger.warning(f"Failed to convert {key} value: {value}")
                                value = None
                            data[key] = value
            
            return data
            
        except Exception as e:
            logger.error(f"Error parsing feature in layer {layer_name}: {str(e)}", exc_info=True)
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
            url = self.url_mapping.get(layer, self.config['url'])
            
            try:
                async with session.get(
                    url, 
                    params=params,
                    timeout=self.request_timeout_config
                ) as response:
                    if response.status != 200:
                        logger.error(f"Error fetching chunk. Status: {response.status}")
                        error_text = await response.text()
                        logger.error(f"Error response: {error_text[:500]}")
                        return None
                    
                    text = await response.text()
                    root = ET.fromstring(text)
                    
                    features = []
                    namespaces = {}
                    for elem in root.iter():
                        if '}' in elem.tag:
                            ns_url = elem.tag.split('}')[0].strip('{')
                            namespaces['ns'] = ns_url
                            break
                    
                    for member in root.findall('.//ns:member', namespaces=namespaces):
                        for feature in member:
                            parsed = self._parse_feature(feature, layer)
                            if parsed and parsed.get('geometry'):
                                features.append(parsed)
                    
                    return features
                    
            except Exception as e:
                logger.error(f"Error fetching chunk: {str(e)}")
                raise

    async def write_to_storage(self, features, dataset):
        """Write features to GeoParquet in Cloud Storage"""
        if not features:
            return
        
        try:
            # Create DataFrame from features
            df = pd.DataFrame(features)
            geometries = [wkt.loads(f['geometry']) for f in features]
            gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:25832")
            
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
            
            # If sync complete, create final files
            if hasattr(self, 'is_sync_complete') and self.is_sync_complete:
                logger.info(f"Sync complete - writing final files")
                
                # Write regular final file
                final_blob = self.bucket.blob(f'raw/{dataset}/current.parquet')
                final_blob.upload_from_filename(temp_working)
                
                # Create dissolved version
                logger.info("Creating dissolved version...")
                try:
                    # Clean geometries before dissolve
                    logger.info("Cleaning geometries before dissolve...")
                    combined_gdf.geometry = combined_gdf.geometry.apply(lambda g: g.buffer(0))
                    
                    # Dissolve cleaned geometries
                    logger.info("Dissolving cleaned geometries...")
                    dissolved = unary_union(combined_gdf.geometry.values)
                    logger.info(f"Dissolved geometry type: {dissolved.geom_type}")
                    
                    if dissolved.geom_type == 'MultiPolygon':
                        logger.info(f"Got MultiPolygon with {len(dissolved.geoms)} parts")
                        # Clean each geometry after dissolve
                        cleaned_geoms = [geom.buffer(0) for geom in dissolved.geoms]
                        dissolved_gdf = gpd.GeoDataFrame(geometry=cleaned_geoms, crs=combined_gdf.crs)
                    else:
                        # Clean single geometry after dissolve
                        cleaned = dissolved.buffer(0)
                        dissolved_gdf = gpd.GeoDataFrame(geometry=[cleaned], crs=combined_gdf.crs)
                    
                    # Let the validator handle final cleaning and transformations
                    logger.info("Validating final dissolved geometries...")
                    dissolved_gdf = validate_and_transform_geometries(dissolved_gdf, f"{dataset}_dissolved")
                    
                    # Write dissolved version
                    temp_dissolved = f"/tmp/{dataset}_dissolved.parquet"
                    dissolved_gdf.to_parquet(temp_dissolved)
                    dissolved_blob = self.bucket.blob(f'raw/{dataset}/dissolved_current.parquet')
                    dissolved_blob.upload_from_filename(temp_dissolved)
                    logger.info("Dissolved version created and saved")
                    
                except Exception as e:
                    logger.error(f"Error during dissolve operation: {str(e)}")
                    raise
                
                # Cleanup
                working_blob.delete()
                os.remove(temp_dissolved)
            
            # Cleanup working file
            if os.path.exists(temp_working):
                os.remove(temp_working)
            
        except Exception as e:
            logger.error(f"Error writing to storage: {str(e)}")
            raise

    async def _fetch_arcgis_features(self, session, layer, url):
        """Fetch features from ArcGIS REST service"""
        try:
            # Extract layer ID from the full layer string
            layer_id = layer.split(':')[1]  # This will get "0" from "Klima_lavbund_demarkation___offentlige_projekter:0"
            
            params = {
                'f': 'json',
                'where': '1=1',
                'outFields': '*',
                'geometryPrecision': '6',
                'outSR': '25832',
                'returnGeometry': 'true'
            }

            # Create SSL context
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            async with session.get(f"{url}/{layer_id}/query", params=params, ssl=ssl_context) as response:
                if response.status != 200:
                    logger.error(f"Error fetching ArcGIS features. Status: {response.status}")
                    error_text = await response.text()
                    logger.error(f"Error response: {error_text[:500]}")
                    return None

                data = await response.json()
                features = []
                
                for feature in data.get('features', []):
                    try:
                        attrs = feature.get('attributes', {})
                        geom = feature.get('geometry', {})
                        
                        if 'rings' not in geom:
                            continue
                            
                        # Convert geometry
                        polygons = []
                        for ring in geom['rings']:
                            coords = [(x, y) for x, y in ring]
                            polygons.append(Polygon(coords))
                        
                        multi_poly = MultiPolygon(polygons) if len(polygons) > 1 else polygons[0]
                        area_ha = multi_poly.area / 10000
                        
                        # Convert timestamps
                        start_date = datetime.fromtimestamp(attrs.get('projektstart')/1000) if attrs.get('projektstart') else None
                        end_date = datetime.fromtimestamp(attrs.get('projektslut')/1000) if attrs.get('projektslut') else None
                        
                        processed_feature = {
                            'layer_name': layer,
                            'geometry': multi_poly.wkt,
                            'area_ha': area_ha,
                            'projektnavn': attrs.get('projektnavn'),
                            'enhedskontakt': attrs.get('enhedskontakt'),
                            'startdato': start_date,
                            'slutdato': end_date,
                            'status': attrs.get('status'),
                            'object_id': attrs.get('OBJECTID'),
                            'global_id': attrs.get('GlobalID')
                        }
                        
                        features.append(processed_feature)
                        
                    except Exception as e:
                        logger.error(f"Error processing feature: {str(e)}")
                        continue
                        
                return features
                
        except Exception as e:
            logger.error(f"Error in _fetch_arcgis_features: {str(e)}")
            return None

    async def sync(self):
        """Sync all water project layers"""
        logger.info("Starting water projects sync...")
        self.is_sync_complete = False
        total_processed = 0
        features_batch = []
        
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                for layer in self.layers:
                    logger.info(f"\nProcessing layer: {layer}")
                    try:
                        service_type = self.service_types.get(layer, 'wfs')
                        base_url = self.url_mapping.get(layer, self.config['url'])

                        if service_type == 'arcgis':
                            features = await self._fetch_arcgis_features(session, layer, base_url)
                            if features:
                                features_batch.extend(features)
                                total_processed += len(features)
                                logger.info(f"Layer {layer}: processed {len(features):,} features")
                            continue

                        # Existing WFS handling code
                        params = self._get_params(layer, 0)
                        async with session.get(base_url, params=params) as response:
                            if response.status != 200:
                                logger.error(f"Failed to fetch {layer}. Status: {response.status}")
                                error_text = await response.text()
                                logger.error(f"Error response: {error_text[:500]}")
                                continue
                            
                            text = await response.text()
                            root = ET.fromstring(text)
                            total_features = int(root.get('numberMatched', '0'))
                            logger.info(f"Layer {layer}: found {total_features:,} total features")
                            
                            # Process first batch
                            features = []
                            namespaces = {}
                            for elem in root.iter():
                                if '}' in elem.tag:
                                    ns_url = elem.tag.split('}')[0].strip('{')
                                    namespaces['ns'] = ns_url
                                    break
                                    
                            for member in root.findall('.//ns:member', namespaces=namespaces):
                                for feature in member:
                                    parsed = self._parse_feature(feature, layer)
                                    if parsed and parsed.get('geometry'):
                                        features.append(parsed)
                            
                            if features:
                                features_batch.extend(features)
                                total_processed += len(features)
                                logger.info(f"Layer {layer}: processed {len(features):,} features")
                            
                            # Process remaining batches
                            for start_index in range(self.batch_size, total_features, self.batch_size):
                                logger.info(f"Layer {layer}: fetching features {start_index:,}-{min(start_index + self.batch_size, total_features):,} of {total_features:,}")
                                chunk = await self._fetch_chunk(session, layer, start_index)
                                if chunk:
                                    features_batch.extend(chunk)
                                    total_processed += len(chunk)
                                    logger.info(f"Layer {layer}: processed {len(chunk):,} features")
                            
                            # Write batch if it's large enough
                            if len(features_batch) >= self.storage_batch_size:
                                logger.info(f"Writing batch of {len(features_batch):,} features")
                                await self.write_to_storage(features_batch, 'water_projects')
                                features_batch = []
                            
                    except Exception as e:
                        logger.error(f"Error processing layer {layer}: {str(e)}", exc_info=True)
                        continue

                # Write any remaining features as final batch
                if features_batch:
                    logger.info(f"Writing final batch of {len(features_batch):,} features")
                    self.is_sync_complete = True
                    await self.write_to_storage(features_batch, 'water_projects')
                
                logger.info(f"Sync completed. Total processed: {total_processed:,}")
                return total_processed
                
        except Exception as e:
            self.is_sync_complete = False
            logger.error(f"Error in sync: {str(e)}", exc_info=True)
            return total_processed

    async def fetch(self):
        """Implement abstract method - using sync() instead"""
        return await self.sync() 
