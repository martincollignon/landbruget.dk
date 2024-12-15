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

def prepare_geometries(gdf, tolerance=0.1):  # tolerance in meters
    """
    Prepare geometries in UTM before conversion to WGS84
    
    Args:
        gdf: GeoDataFrame with geometries in any CRS
        tolerance: Simplification tolerance in meters
        
    Returns:
        GeoDataFrame with valid geometries in EPSG:4326
    """
    logger.info(f"Starting with {len(gdf)} geometries in CRS: {gdf.crs}")
    
    # Ensure we're in UTM
    if gdf.crs != "EPSG:25832":
        gdf = gdf.to_crs("EPSG:25832")
        logger.info("Converted to UTM (EPSG:25832)")
    
    # Initial cleanup in UTM
    gdf.geometry = gdf.geometry.apply(lambda g: g.buffer(0))
    
    # Validate initial state
    invalid_before = ~gdf.geometry.is_valid
    if invalid_before.any():
        logger.warning(f"Found {invalid_before.sum()} invalid geometries before cleanup")
    
    # Clean and simplify in UTM
    original_areas = gdf.geometry.area
    gdf.geometry = gdf.geometry.apply(lambda g: g.buffer(0).simplify(tolerance))
    new_areas = gdf.geometry.area
    
    # Check area changes
    area_changes = ((new_areas - original_areas) / original_areas * 100)
    max_change = area_changes.abs().max()
    logger.info(f"Maximum area change during cleanup: {max_change:.6f}%")
    
    if max_change > 1.0:  # More than 1% change
        logger.warning("Large area changes detected during cleanup")
        
    # Validate in UTM
    invalid_after = ~gdf.geometry.is_valid
    if invalid_after.any():
        raise ValueError(f"Found {invalid_after.sum()} invalid geometries after UTM cleanup")
    
    # Convert to WGS84
    logger.info("Converting to WGS84...")
    wgs84_gdf = gdf.to_crs("EPSG:4326")
    
    # Final cleanup in WGS84
    wgs84_gdf.geometry = wgs84_gdf.geometry.apply(lambda g: g.buffer(0))
    
    # Final validation
    invalid_wgs84 = ~wgs84_gdf.geometry.is_valid
    if invalid_wgs84.any():
        raise ValueError(f"Found {invalid_wgs84.sum()} invalid geometries after WGS84 conversion")
    
    # Check for self-intersections
    self_intersecting = ~wgs84_gdf.geometry.is_simple
    if self_intersecting.any():
        logger.warning(f"Found {self_intersecting.sum()} self-intersecting geometries in WGS84")
        
    logger.info("Geometry preparation complete")
    return wgs84_gdf