import geopandas as gpd
import logging
import numpy as np
from shapely.validation import make_valid # requires shapely-1.8.2 and basically does things like convert polygons to multipolygons

from geo_py_utils.geo_general.geo_utils import explode_multiparts

logger = logging.getLogger(__file__)

def make_valid_gpd(
        shp: gpd.GeoDataFrame,
        unsafely_remove_violations=False
    ) -> gpd.GeoDataFrame:

    """Make each geometry valid in a geodf

    Args:
        shp (gpd.GeoDataFrame): geodataframe to use 

    Returns:
        gpd.GeoDataFrame: geodataframe with POINT geometry
    """
    assert isinstance(shp, gpd.GeoDataFrame)

    # Try to make each geometry valid
    shp.geometry = [make_valid(g) for g in shp.geometry]

    # Additional step : if we really want
    if unsafely_remove_violations:
        n_before = shp.shape[0]
        idx_to_drop = ~shp.geometry.is_valid
        shp = shp.loc[~idx_to_drop, ]
        n_after = shp.shape[0]
        n_removed = n_before - n_after

        if n_removed > 0:  print( f'Removed {n_removed} invalid features that could not be fixed by shapely.validation.make_valid()')
        if shp.shape[0] == 0:  print('Warning! no more rows in geodf')

    return shp






def make_extra_valid_gpd(
        gdf: gpd.GeoDataFrame, 
        thresh_area_m = 0.5,
        buffer_size=10**-6,
        sliver_ratio_thres = 10**4,
        explode_multiparts = False,
        list_geoms_types_to_keep = None,
        **make_valid_kwargs
        ):

    # no GeometryCollection
    if explode_multiparts:
        gdf = explode_multiparts(gdf)

    # get the number of rows after the geometrycollection explosion
    row_init = gdf.shape[0]

    cols_to_drop = np.intersect1d( ['level_0', 'index'] ,  gdf.columns)
    if len(cols_to_drop):
        gdf.drop(columns=cols_to_drop, inplace=True)

    # only certain type
    if list_geoms_types_to_keep:
        gdf = gdf[gdf.geom_type.isin(list_geoms_types_to_keep)]

    # invalid polygons
    gdf = make_valid_gpd(gdf, **make_valid_kwargs)
    
    # missing geometries
    gdf = gdf[~gdf.geometry.isna()]

    # slither polyons + 0 area polygons
    # https://gis.stackexchange.com/questions/263696/invalid-geometries-made-valid-dont-remain-valid
    gdf = gdf[gdf.geometry.area > 0]
    gdf = gdf[(gdf.geometry.length/gdf.geometry.area) <= sliver_ratio_thres]

    # small buffer
    init_crs = gdf.crs
    gdf_tmp = gdf.to_crs(3347)
    gdf_tmp['geometry'] = gdf_tmp.geometry.buffer(buffer_size)
    gdf= gdf_tmp.to_crs(init_crs)

    # area too small
    if thresh_area_m and thresh_area_m > 0:
        init_crs = gdf.crs
        gdf_tmp = gdf.to_crs(3347)
        gdf_tmp = gdf_tmp[gdf_tmp.area > thresh_area_m]

        gdf= gdf_tmp.to_crs(init_crs)

    row_end = gdf.shape[0]

    if row_init != row_end:
        logger.warning(f'Warning! lost {row_init-row_end} rows because of gdf cleanup')

    return gdf
