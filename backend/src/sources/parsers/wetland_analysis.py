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
            valid_cadastral AS (
                SELECT 
                    bfe_number,
                    -- Make valid and clean up any geometry issues
                    ST_MakeValid(ST_Buffer(geometry, 0)) as geometry
                FROM cadastral_properties
            ),
            valid_wetlands AS (
                SELECT 
                    -- Make valid and clean up any geometry issues
                    ST_MakeValid(ST_Buffer(geometry, 0)) as geometry
                FROM wetlands
            ),
            valid_water_projects AS (
                SELECT 
                    -- Make valid and clean up any geometry issues
                    ST_MakeValid(ST_Buffer(geometry, 0)) as geometry
                FROM water_projects_combined
            ),
            bfe_wetlands AS (
                SELECT 
                    cp.bfe_number,
                    cp.geometry as bfe_geometry,
                    -- Use ST_MakeValid on the intersection result
                    ST_MakeValid(ST_Intersection(cp.geometry, w.geometry)) as wetland_geometry
                FROM valid_cadastral cp
                LEFT JOIN valid_wetlands w ON ST_Intersects(cp.geometry, w.geometry)
                WHERE ST_IsValid(cp.geometry) AND (w.geometry IS NULL OR ST_IsValid(w.geometry))
            ),
            wetland_projects AS (
                SELECT 
                    bw.bfe_number,
                    -- Use ST_MakeValid on the intersection result
                    ST_MakeValid(ST_Intersection(bw.wetland_geometry, wpc.geometry)) as wetland_project_geometry
                FROM bfe_wetlands bw
                LEFT JOIN valid_water_projects wpc 
                ON ST_Intersects(bw.wetland_geometry, wpc.geometry)
                WHERE ST_IsValid(bw.wetland_geometry) AND (wpc.geometry IS NULL OR ST_IsValid(wpc.geometry))
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
                CAST(COALESCE(ST_Area(ST_Union(bw.wetland_geometry)) / 
                    NULLIF(ST_Area(bw.bfe_geometry), 0) * 100, 0) AS NUMERIC(5,2)) as wetland_share_pct,
                CAST(COALESCE(ST_Area(ST_Union(wp.wetland_project_geometry)) / 
                    NULLIF(ST_Area(ST_Union(bw.wetland_geometry)), 0) * 100, 0) AS NUMERIC(5,2)) as wetland_project_share_pct,
                CAST((ST_Area(bw.bfe_geometry) - COALESCE(ST_Area(ST_Union(bw.wetland_geometry)), 0)) / 
                    ST_Area(bw.bfe_geometry) * 100 AS NUMERIC(5,2)) as non_wetland_share_pct,
                CAST(ST_Area(bw.bfe_geometry) AS NUMERIC(20,2)) as total_area_m2,
                CAST(COALESCE(ST_Area(ST_Union(bw.wetland_geometry)), 0) AS NUMERIC(20,2)) as wetland_area_m2,
                CAST(COALESCE(ST_Area(ST_Union(wp.wetland_project_geometry)), 0) AS NUMERIC(20,2)) as wetland_project_area_m2,
                CAST((ST_Area(bw.bfe_geometry) - 
                    COALESCE(ST_Area(ST_Union(bw.wetland_geometry)), 0)) AS NUMERIC(20,2)) as non_wetland_area_m2,
                ST_MakeValid(bw.bfe_geometry) as bfe_geometry,
                ST_MakeValid(ST_Union(bw.wetland_geometry)) as wetland_geometry,
                ST_MakeValid(ST_Union(wp.wetland_project_geometry)) as wetland_project_geometry,
                ST_MakeValid(ST_Difference(bw.bfe_geometry, ST_Union(bw.wetland_geometry))) as non_wetland_geometry
            FROM bfe_wetlands bw
            LEFT JOIN wetland_projects wp ON bw.bfe_number = wp.bfe_number
            WHERE ST_IsValid(bw.bfe_geometry)
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