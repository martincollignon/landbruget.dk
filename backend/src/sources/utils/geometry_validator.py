from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.polygon import orient
import geopandas as gpd
import logging

logger = logging.getLogger(__name__)

def validate_and_transform_geometries(gdf: gpd.GeoDataFrame, dataset_name: str) -> gpd.GeoDataFrame:
    """
    Validates and transforms geometries while preserving original areas.
    
    Args:
        gdf: GeoDataFrame with geometries in EPSG:25832
        dataset_name: Name of dataset for logging
    
    Returns:
        GeoDataFrame with valid geometries in EPSG:4326
    """
    try:
        initial_count = len(gdf)
        
        # Basic validation
        logger.info(f"{dataset_name}: Starting validation with {initial_count} features")
        logger.info(f"{dataset_name}: Input CRS: {gdf.crs}")
        
        # Ensure we're in the correct projected CRS for area calculations
        if gdf.crs is None:
            logger.warning(f"{dataset_name}: No CRS found, assuming EPSG:25832")
            gdf.set_crs(epsg=25832, inplace=True)
        elif gdf.crs.to_epsg() != 25832:
            logger.info(f"{dataset_name}: Converting from {gdf.crs} to EPSG:25832")
            gdf = gdf.to_crs(epsg=25832)
        
        # Fix invalid geometries and ensure proper orientation
        invalid_mask = ~gdf.geometry.is_valid
        if invalid_mask.any():
            logger.warning(f"{dataset_name}: Found {invalid_mask.sum()} invalid geometries. Attempting to fix...")
            gdf.loc[invalid_mask, 'geometry'] = gdf.loc[invalid_mask, 'geometry'].apply(
                lambda geom: orient(geom.buffer(0), sign=1.0) if isinstance(geom, (Polygon, MultiPolygon)) else geom.buffer(0)
            )
        
        # Ensure proper orientation for all polygon geometries
        logger.info(f"{dataset_name}: Ensuring proper orientation for all geometries")
        gdf['geometry'] = gdf.geometry.apply(
            lambda geom: orient(geom, sign=1.0) if isinstance(geom, (Polygon, MultiPolygon)) else geom
        )
        
        # Remove nulls and empty geometries
        gdf = gdf.dropna(subset=['geometry'])
        gdf = gdf[~gdf.geometry.is_empty]
        
        # Calculate areas in projected CRS (EPSG:25832)
        gdf['area_m2'] = gdf.geometry.area
        
        # Transform to WGS84
        logger.info(f"{dataset_name}: Converting to EPSG:4326")
        gdf = gdf.to_crs("EPSG:4326")
        logger.info(f"{dataset_name}: Output CRS: {gdf.crs}")
        
        final_count = len(gdf)
        removed_count = initial_count - final_count
        
        logger.info(f"{dataset_name}: Validation complete")
        logger.info(f"{dataset_name}: Initial features: {initial_count}")
        logger.info(f"{dataset_name}: Valid features: {final_count}")
        logger.info(f"{dataset_name}: Removed features: {removed_count}")
        
        return gdf
        
    except Exception as e:
        logger.error(f"{dataset_name}: Error in geometry validation: {str(e)}")
        raise 