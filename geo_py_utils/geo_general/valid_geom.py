
from shapely.validation import make_valid # requires shapely-1.8.2 and basically does things like convert polygons to multipolygons
import geopandas as gpd


def make_valid_gpd(shp: gpd.GeoDataFrame,
                   unsafely_remove_violations=False) -> gpd.GeoDataFrame:
    """Make each geometry valid in a geodf


    Args:
        shp (gpd.GeoDataFrame): geodataframe to use 

    Returns:
        gpd.GeoDataFrame: geodataframe with POINT geometry
    """
    assert isinstance(shp, gpd.GeoDataFrame)

    # Try to make each geometry valid
    shp['geometry'] = [make_valid(g) for g in shp['geometry']]

    # Additional step : if we really want
    if unsafely_remove_violations:
        n_before = shp.shape[0]
        idx_to_drop = ~shp['geometry'].is_valid
        shp = shp.loc[~idx_to_drop, ]
        n_after = shp.shape[0]
        n_removed = n_before - n_after

        if n_removed > 0:  print( 'Removed {n_removed} invalid features that could not be fixed by shapely.validation.make_valid()')
        if shp.shape[0] == 0:  print('Warning! no more rows in geodf')

    return shp
