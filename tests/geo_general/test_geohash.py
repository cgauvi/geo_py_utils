import geopandas as gpd
import geohash
from shapely.geometry import shape, Polygon, Point
from os.path import join

from gic.constants import Projet_paths

import sys
sys.path.append(join(Projet_paths.PATH_TO_GIC, 'geo_py_utils'))

from geo_py_utils.geo_general.bbox import get_list_coordinates_from_bbox, get_bbox_centroid
from geo_py_utils.geo_general.geo_utils import get_geodataframe_from_list_coord
from geo_py_utils.geo_general.map import map_simple
from geo_py_utils.geo_general.geohash_utils import (
    recursively_partition_geohash_cells, 
    get_geohash_box,
    get_all_geohash_from_gdf,
    geohash_max_precision
)


def test_basic_geohash_utils():


    GEOHASH_PRECISION = 6
 
    # Getting all geohash from extent
    geohash_single_index = geohash.encode(46.5, -71.3, GEOHASH_PRECISION)

    # Get a list of 4 points: bl, tl, tr, br
    geohash_bbox_coord_list = get_geohash_box(geohash_single_index)
    assert len(geohash_bbox_coord_list) == 4

    # Can convert the list of coordinates to gpd.GeoDataFrame
    shp_box = get_geodataframe_from_list_coord([geohash_bbox_coord_list], crs=4326)
    assert isinstance(shp_box, gpd.GeoDataFrame)

    # Make sure all returned polygons are within the envelope of the original polygon
    shp = gpd.GeoDataFrame({'id': [0]},
                           geometry=[Polygon(((-71.5, 46.5),
                                              (-71.6, 46.4),
                                              (-71.5, 46.7),
                                              (-71.5, 46.5)))
                                     ],
                           crs=4326
                           )

    # `get_all_geohash_from_gdf` is pooly named: shoud be `get_all_geohash_from_gdf_envevelope` since 
    # it is really just a wrapper over `get_all_geohash_from_extent`
    shp_geohash = get_all_geohash_from_gdf(shp, GEOHASH_PRECISION,predicate='within')
    is_within = shp_geohash.dissolve().envelope.within(shp.envelope).values[0]
    assert is_within



def test_get_max_precision():

    # Max precision
    # Quebec city
    qc_city_extents = {"east": -70.69047684581989,
                       "north": 47.308868412273725,
                       "south": 46.52872508839602,
                       "west": -71.81461940388115}

    # Max precision should be 3 and no errors
    shp_qc_city = get_geodataframe_from_list_coord(
        [get_list_coordinates_from_bbox(qc_city_extents)], crs=4326)
    max_prec_qc = geohash_max_precision(shp_qc_city)

    assert max_prec_qc == 2

    # Entire US - not just CONUS, the entire thing with puerto rico et al.
    shp_us = gpd.read_file(
        'https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_nation_5m.zip')

    # Max precision should be 1 and should get a warning since no single geohash cell is large enough to cover the entire region
    max_prec_us = geohash_max_precision(shp_us)
 
    assert max_prec_us == 1
    


def test_recursive_geohash():


    shp_all_1 = gpd.GeoDataFrame(
        {
            'id': [0,1],
            "geometry": [Point (-71.16451, 46.86968),   Point (-71.25148, 48.41987)	],
            "counts": [1.0, 1.0]
    }     ,
    crs = 4326
    )

    shp_part, _ = recursively_partition_geohash_cells(shp_all_1,
                                                    count_column_name='counts',
                                                    thresh_num_points = 1)


if __name__ == '__main__':


    test_recursive_geohash()