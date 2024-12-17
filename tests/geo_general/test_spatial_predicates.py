import geopandas as gpd
from shapely.geometry import Polygon

from geo_py_utils.geo_general.geo_utils import compare_two_gdf_predicate
from geo_py_utils.geo_general.geohash_utils import get_geohash_box

def test_all_spatial_predicates():

    list_gh_5_strictly_within = ['f2kbw', 'f2kbm']
    list_gh_5_within = list_gh_5_strictly_within.copy()
    list_gh_5_within.extend([ 'f2kbx', 'f2kbz'])

    list_gh_5_touch_exterior_only = ['f2m08']
    
    list_gh_4_parent = [ 'f2kb' ]


    gdf_gh_5_within = gpd.GeoDataFrame(
        {'geohash_index': list_gh_5_within},
        geometry = gpd.GeoSeries([Polygon(get_geohash_box(h)) for h in list_gh_5_within]),
        crs=4326
    )

    gdf_gh_5_strictly_within = gpd.GeoDataFrame(
        {'geohash_index': list_gh_5_strictly_within},
        geometry = gpd.GeoSeries([Polygon(get_geohash_box(h)) for h in list_gh_5_strictly_within]),
        crs=4326
    )

    gdf_gh_5_intersects = gpd.GeoDataFrame(
        {'geohash_index': [*list_gh_5_touch_exterior_only, *list_gh_5_within]},
        geometry = gpd.GeoSeries([Polygon(get_geohash_box(h)) for h in [*list_gh_5_touch_exterior_only, *list_gh_5_within]]),
        crs=4326
    )
    gdf_gh_4_parent = gpd.GeoDataFrame(
        {'geohash_index': list_gh_4_parent},
        geometry = gpd.GeoSeries([Polygon(get_geohash_box(h)) for h in list_gh_4_parent]),
        crs=4326
    )


    assert compare_two_gdf_predicate(gdf_gh_5_within, gdf_gh_4_parent, 'within').sum() == gdf_gh_5_within.shape[0]
    assert compare_two_gdf_predicate(gdf_gh_5_within, gdf_gh_4_parent, 'within', any).sum() == gdf_gh_5_within.shape[0]

    assert compare_two_gdf_predicate(gdf_gh_5_within, gdf_gh_4_parent, 'strictly within', any).sum() == gdf_gh_5_strictly_within.shape[0]
    assert compare_two_gdf_predicate(gdf_gh_5_within, gdf_gh_4_parent, 'strictly within', all).sum() == gdf_gh_5_strictly_within.shape[0]
    
    assert compare_two_gdf_predicate(gdf_gh_5_intersects, gdf_gh_4_parent, 'intersects').sum() == gdf_gh_5_intersects.shape[0]
    
    assert compare_two_gdf_predicate(gdf_gh_5_strictly_within, gdf_gh_4_parent, 'touches').sum() == 0 
    assert compare_two_gdf_predicate(gdf_gh_5_intersects, gdf_gh_4_parent, 'touches').sum() == len(list_gh_5_touch_exterior_only)

    assert compare_two_gdf_predicate(gdf_gh_4_parent, gdf_gh_5_within, 'contains').sum() == gdf_gh_4_parent.shape[0]
    assert compare_two_gdf_predicate(gdf_gh_4_parent, gdf_gh_5_within, 'strictly contains', all).sum() == gdf_gh_4_parent.shape[0] - 1
    assert compare_two_gdf_predicate(gdf_gh_4_parent, gdf_gh_5_within, 'strictly contains', any).sum() == gdf_gh_4_parent.shape[0]



if __name__ == '__main__':

    test_all_spatial_predicates()

# %%