from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.polygon import orient
from shapely.validation import explain_validity
import geopandas as gpd
import logging
from shapely.wkt import loads, dumps
from shapely.ops import unary_union

logger = logging.getLogger(__name__)

def fix_for_bigquery(geom):
    """
    Attempt to fix geometry issues that would make it invalid for BigQuery:
    - Remove duplicate vertices
    - Fix self-intersections
    - Ensure proper ring orientation
    - Fix edge crossings
    
    Returns fixed geometry or None if unfixable
    """
    try:
        if not isinstance(geom, (Polygon, MultiPolygon)):
            return geom
            
        # First try basic buffer(0) to fix self-intersections
        cleaned = geom.buffer(0)
        if not cleaned.is_valid:
            return None
            
        # Convert to WKT and back to normalize
        cleaned = loads(dumps(cleaned))
        
        # Handle MultiPolygon vs Polygon
        polygons = cleaned.geoms if isinstance(cleaned, MultiPolygon) else [cleaned]
        fixed_polys = []
        
        for poly in polygons:
            # Fix exterior ring
            ext_coords = list(poly.exterior.coords)
            # Remove duplicate consecutive vertices
            ext_coords = [ext_coords[i] for i in range(len(ext_coords)) 
                        if i == 0 or ext_coords[i] != ext_coords[i-1]]
            # Ensure ring is closed
            if ext_coords[0] != ext_coords[-1]:
                ext_coords.append(ext_coords[0])
            
            # Fix interior rings
            int_rings = []
            for interior in poly.interiors:
                int_coords = list(interior.coords)
                # Remove duplicate consecutive vertices
                int_coords = [int_coords[i] for i in range(len(int_coords))
                            if i == 0 or int_coords[i] != int_coords[i-1]]
                # Ensure ring is closed
                if int_coords[0] != int_coords[-1]:
                    int_coords.append(int_coords[0])
                if len(int_coords) >= 4:  # Only keep valid rings
                    int_rings.append(int_coords)
            
            # Create new polygon with fixed rings
            if len(ext_coords) >= 4:
                try:
                    fixed_poly = Polygon(ext_coords, int_rings)
                    if fixed_poly.is_valid:
                        fixed_polys.append(fixed_poly)
                except Exception as e:
                    logger.warning(f"Could not create polygon: {str(e)}")
                    continue
        
        if not fixed_polys:
            return None
            
        # Create final geometry
        final_geom = MultiPolygon(fixed_polys) if len(fixed_polys) > 1 else fixed_polys[0]
        
        # Ensure proper orientation
        final_geom = orient(final_geom, sign=1.0)
        
        # Final validity check
        if not final_geom.is_valid:
            # Try one last unary_union as a last resort
            final_geom = unary_union([final_geom])
            if not final_geom.is_valid:
                return None
        
        return final_geom
        
    except Exception as e:
        logger.error(f"Error fixing geometry for BigQuery: {str(e)}")
        return None

def is_valid_for_bigquery(geom) -> bool:
    """
    Check if geometry meets BigQuery geography requirements:
    - No self-intersections
    - Proper ring orientation
    - No duplicate vertices
    - No empty rings
    - Edges can't cross
    """
    try:
        # Convert to WKT and back to catch any edge crossing issues
        wkt = dumps(geom)
        test_geom = loads(wkt)
        
        if not test_geom.is_valid:
            return False
            
        if isinstance(test_geom, (Polygon, MultiPolygon)):
            # Check each polygon
            polygons = test_geom.geoms if isinstance(test_geom, MultiPolygon) else [test_geom]
            
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
        
        # Check and fix BigQuery compatibility
        logger.info(f"{dataset_name}: Checking BigQuery compatibility")
        bq_valid_mask = gdf.geometry.apply(is_valid_for_bigquery)
        if not bq_valid_mask.all():
            invalid_count = (~bq_valid_mask).sum()
            logger.warning(f"{dataset_name}: Found {invalid_count} geometries not valid for BigQuery. Attempting to fix...")
            
            # Try to fix invalid geometries
            invalid_indices = gdf[~bq_valid_mask].index
            fixed_geometries = gdf.loc[invalid_indices, 'geometry'].apply(fix_for_bigquery)
            
            # Update fixed geometries and remove unfixable ones
            fixed_mask = ~fixed_geometries.isna()
            if fixed_mask.any():
                gdf.loc[invalid_indices[fixed_mask], 'geometry'] = fixed_geometries[fixed_mask]
            
            # Remove remaining invalid geometries
            gdf = gdf[gdf.geometry.apply(is_valid_for_bigquery)]
        
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