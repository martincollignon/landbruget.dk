from pathlib import Path
import sys
import asyncio
import os
import xml.etree.ElementTree as ET
from tqdm import tqdm
import geopandas as gpd
import aiohttp
from dotenv import load_dotenv

from ...base import Source, clean_value

class Cadastral(Source):
    def __init__(self, config):
        super().__init__(config)
        load_dotenv()
        self.username = os.getenv('DATAFORDELER_USERNAME')
        self.password = os.getenv('DATAFORDELER_PASSWORD')
        if not self.username or not self.password:
            raise ValueError("Missing DATAFORDELER_USERNAME or DATAFORDELER_PASSWORD environment variables")
        self.page_size = 10000
        self.max_concurrent = 20
        self.namespaces = {
            'wfs': 'http://www.opengis.net/wfs/2.0',
            'mat': 'http://data.gov.dk/schemas/matrikel/1',
            'gml': 'http://www.opengis.net/gml/3.2'
        }

    async def sync(self, client):
        """Sync cadastral data to PostgreSQL database"""
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
                # Create the SQL query for batch upsert
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
                        id_namespace = EXCLUDED.id_namespace,
                        id_local = EXCLUDED.id_local,
                        registration_from = EXCLUDED.registration_from,
                        effect_from = EXCLUDED.effect_from,
                        authority = EXCLUDED.authority,
                        is_worker_housing = EXCLUDED.is_worker_housing,
                        is_common_lot = EXCLUDED.is_common_lot,
                        has_owner_apartments = EXCLUDED.has_owner_apartments,
                        is_separated_road = EXCLUDED.is_separated_road,
                        agricultural_notation = EXCLUDED.agricultural_notation,
                        geometry = ST_GeomFromText(EXCLUDED.geometry, 25832)
                """
                # Execute batch upsert
                await client.executemany(query, [
                    (
                        r['bfe_number'], r['business_event'], r['business_process'],
                        r['latest_case_id'], r['id_namespace'], r['id_local'],
                        r['registration_from'], r['effect_from'], r['authority'],
                        r['is_worker_housing'], r['is_common_lot'], r['has_owner_apartments'],
                        r['is_separated_road'], r['agricultural_notation'], r['geometry']
                    ) for r in batch
                ])
                pbar.update(len(batch))
                
        print("\nSync completed!")
        return total_records
