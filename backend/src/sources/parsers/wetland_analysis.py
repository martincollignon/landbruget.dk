from pathlib import Path
import logging
from ...base import Source

logger = logging.getLogger(__name__)

class WetlandAnalysis(Source):
    def __init__(self, config):
        super().__init__(config)
        
    async def _create_tables(self, client):
        """Create necessary database tables"""
        await client.execute("""
            CREATE TABLE IF NOT EXISTS bfe_wetland_analysis (
                bfe_number INTEGER PRIMARY KEY,
                wetland_share_pct NUMERIC,
                wetland_project_share_pct NUMERIC,
                non_wetland_share_pct NUMERIC,
                total_area_m2 NUMERIC,
                wetland_area_m2 NUMERIC,
                wetland_project_area_m2 NUMERIC,
                non_wetland_area_m2 NUMERIC,
                bfe_geometry GEOMETRY(MULTIPOLYGON, 25832),
                wetland_geometry GEOMETRY(MULTIPOLYGON, 25832),
                wetland_project_geometry GEOMETRY(MULTIPOLYGON, 25832),
                non_wetland_geometry GEOMETRY(MULTIPOLYGON, 25832),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS bfe_wetland_analysis_geometry_idx 
            ON bfe_wetland_analysis USING GIST (bfe_geometry);
        """)

    async def sync(self, client):
        """Run the wetland analysis"""
        try:
            logger.info("Starting wetland analysis...")
            
            # Create tables if they don't exist
            await self._create_tables(client)
            
            # Set statement timeout to 2 hours
            await client.execute("SET statement_timeout TO '7200000'")
            
            # Run the analysis
            analysis_query = """
            WITH 
            bfe_wetlands AS (
                SELECT 
                    cp.bfe_number,
                    cp.geometry as bfe_geometry,
                    ST_Intersection(cp.geometry, w.geometry) as wetland_geometry
                FROM cadastral_properties cp
                LEFT JOIN wetlands w ON ST_Intersects(cp.geometry, w.geometry)
            ),
            wetland_projects AS (
                SELECT 
                    bw.bfe_number,
                    ST_Intersection(bw.wetland_geometry, wpc.geometry) as wetland_project_geometry
                FROM bfe_wetlands bw
                LEFT JOIN water_projects_combined wpc 
                ON ST_Intersects(bw.wetland_geometry, wpc.geometry)
            )
            INSERT INTO bfe_wetland_analysis (
                bfe_number, wetland_share_pct, wetland_project_share_pct, 
                non_wetland_share_pct, total_area_m2, wetland_area_m2, 
                wetland_project_area_m2, non_wetland_area_m2, 
                bfe_geometry, wetland_geometry, wetland_project_geometry, 
                non_wetland_geometry
            )
            SELECT 
                bw.bfe_number,
                ROUND((COALESCE(ST_Area(ST_Union(bw.wetland_geometry)), 0) / 
                    NULLIF(ST_Area(bw.bfe_geometry), 0) * 100)::numeric, 2) as wetland_share_pct,
                ROUND((COALESCE(ST_Area(ST_Union(wp.wetland_project_geometry)), 0) / 
                    NULLIF(ST_Area(ST_Union(bw.wetland_geometry)), 0) * 100)::numeric, 2) as wetland_project_share_pct,
                ROUND((ST_Area(bw.bfe_geometry) - COALESCE(ST_Area(ST_Union(bw.wetland_geometry)), 0)) / 
                    ST_Area(bw.bfe_geometry) * 100::numeric, 2) as non_wetland_share_pct,
                ROUND(ST_Area(bw.bfe_geometry)::numeric, 2) as total_area_m2,
                ROUND(COALESCE(ST_Area(ST_Union(bw.wetland_geometry)), 0)::numeric, 2) as wetland_area_m2,
                ROUND(COALESCE(ST_Area(ST_Union(wp.wetland_project_geometry)), 0)::numeric, 2) as wetland_project_area_m2,
                ROUND((ST_Area(bw.bfe_geometry) - 
                    COALESCE(ST_Area(ST_Union(bw.wetland_geometry)), 0))::numeric, 2) as non_wetland_area_m2,
                bw.bfe_geometry,
                ST_Union(bw.wetland_geometry) as wetland_geometry,
                ST_Union(wp.wetland_project_geometry) as wetland_project_geometry,
                ST_Difference(bw.bfe_geometry, ST_Union(bw.wetland_geometry)) as non_wetland_geometry
            FROM bfe_wetlands bw
            LEFT JOIN wetland_projects wp ON bw.bfe_number = wp.bfe_number
            GROUP BY bw.bfe_number, bw.bfe_geometry
            ON CONFLICT (bfe_number) 
            DO UPDATE SET 
                wetland_share_pct = EXCLUDED.wetland_share_pct,
                wetland_project_share_pct = EXCLUDED.wetland_project_share_pct,
                non_wetland_share_pct = EXCLUDED.non_wetland_share_pct,
                total_area_m2 = EXCLUDED.total_area_m2,
                wetland_area_m2 = EXCLUDED.wetland_area_m2,
                wetland_project_area_m2 = EXCLUDED.wetland_project_area_m2,
                non_wetland_area_m2 = EXCLUDED.non_wetland_area_m2,
                bfe_geometry = EXCLUDED.bfe_geometry,
                wetland_geometry = EXCLUDED.wetland_geometry,
                wetland_project_geometry = EXCLUDED.wetland_project_geometry,
                non_wetland_geometry = EXCLUDED.non_wetland_geometry,
                updated_at = CURRENT_TIMESTAMP
            RETURNING bfe_number;
            """
            
            result = await client.fetch(analysis_query)
            total_analyzed = len(result)
            logger.info(f"Analysis completed. Total records analyzed: {total_analyzed:,}")
            
            return total_analyzed
            
        except Exception as e:
            logger.error(f"Error during analysis: {str(e)}")
            raise
        finally:
            # Reset statement timeout
            await client.execute("RESET statement_timeout")

    async def fetch(self):
        """Not implemented - using sync() directly"""
        raise NotImplementedError("This source uses sync() directly") 