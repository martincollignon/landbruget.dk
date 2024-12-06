from shapely.geometry import Polygon, MultiPolygon
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
        
        # Fix invalid geometries
        invalid_mask = ~gdf.geometry.is_valid
        if invalid_mask.any():
            logger.warning(f"{dataset_name}: Found {invalid_mask.sum()} invalid geometries. Attempting to fix...")
            gdf.loc[invalid_mask, 'geometry'] = gdf.loc[invalid_mask, 'geometry'].apply(
                lambda geom: geom.buffer(0) if geom else None
            )
        
        # Remove nulls and empty geometries
        gdf = gdf.dropna(subset=['geometry'])
        gdf = gdf[~gdf.geometry.is_empty]
        
        # Calculate areas in original projection (EPSG:25832)
        gdf['area_m2'] = gdf.geometry.area
        
        # Transform to WGS84
        gdf = gdf.to_crs("EPSG:4326")
        
        # Final validation check
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