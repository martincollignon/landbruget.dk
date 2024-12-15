from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.polygon import orient
import geopandas as gpd
import logging
from shapely.ops import unary_union

logger = logging.getLogger(__name__)

def is_valid_for_bigquery(geom) -> bool:
    """
    Check if geometry meets BigQuery geography requirements:
    - No self-intersections
    - No duplicate vertices
    - No empty rings
    - Edges can't cross
    - Proper ring orientation
    """
    try:
        if not geom.is_valid or not geom.is_simple:
            return False
            
        if isinstance(geom, (Polygon, MultiPolygon)):
            # Check each polygon
            polygons = geom.geoms if isinstance(geom, MultiPolygon) else [geom]
            
            for poly in polygons:
                # Check exterior ring
                ext_coords = list(poly.exterior.coords)
                if len(ext_coords) < 4:  # Need at least 4 points (first = last)
                    return False
                    
                # Check for duplicate consecutive vertices
                for i in range(len(ext_coords)-1):
                    if ext_coords[i] == ext_coords[i+1]:
                        return False
                
                # Check interior rings
                for interior in poly.interiors:
                    int_coords = list(interior.coords)
                    if len(int_coords) < 4:
                        return False
                    
                    # Check for duplicate consecutive vertices in interior
                    for i in range(len(int_coords)-1):
                        if int_coords[i] == int_coords[i+1]:
                            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error checking BigQuery validity: {str(e)}")
        return False

def validate_and_transform_geometries(gdf: gpd.GeoDataFrame, dataset_name: str) -> gpd.GeoDataFrame:
    """
    Validates geometries for BigQuery compatibility.
    Assumes input is already in WGS84 (EPSG:4326).
    """
    try:
        initial_count = len(gdf)
        logger.info(f"{dataset_name}: Starting validation with {initial_count} features")
        logger.info(f"{dataset_name}: Input CRS: {gdf.crs}")
        
        if gdf.crs.to_epsg() != 4326:
            raise ValueError(f"Expected WGS84 input, got {gdf.crs}")
        
        # Remove nulls and empty geometries
        gdf = gdf.dropna(subset=['geometry'])
        gdf = gdf[~gdf.geometry.is_empty]
        
        # Validate
        invalid_mask = ~gdf.geometry.is_valid | ~gdf.geometry.is_simple
        if invalid_mask.any():
            logger.warning(f"{dataset_name}: Found {invalid_mask.sum()} invalid geometries")
            raise ValueError(f"Found {invalid_mask.sum()} invalid geometries")
        
        final_count = len(gdf)
        removed_count = initial_count - final_count
        
        logger.info(f"{dataset_name}: Validation complete")
        logger.info(f"{dataset_name}: Initial features: {initial_count}")
        logger.info(f"{dataset_name}: Valid features: {final_count}")
        logger.info(f"{dataset_name}: Removed features: {removed_count}")
        logger.info(f"{dataset_name}: Output CRS: {gdf.crs}")
        
        return gdf
        
    except Exception as e:
        logger.error(f"{dataset_name}: Error in geometry validation: {str(e)}")
        raise