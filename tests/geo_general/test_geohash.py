import geopandas as gpd
import geohash
from shapely.geometry import Polygon, Point
from os.path import join

from geo_py_utils.geo_general.bbox import get_list_coordinates_from_bbox
from geo_py_utils.geo_general.geo_utils import get_geodataframe_from_list_coord
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
    


def test_recursive_geohash_refines():


    shp_all_1 = gpd.GeoDataFrame(
        {
            'id': [0,1 ,2],
            "geometry": [Point (-71.16451, 46.86968),   Point (-71.25148, 48.41987), Point (-71.25148, 48.419)	], #slight tweak - same geohash 
            "counts": [1.0, 1.0, 1.0]
    }   ,
    crs = 4326
    )

    shp_part, _ = recursively_partition_geohash_cells(shp_all_1,
                                                    count_column_name='counts',
                                                    min_num_points=1,
                                                    max_precision=4)

    assert shp_part[shp_part['counts'] > 0].shape[0] == shp_all_1.shape[0]-1 # aggregated 2 geohashes into one (last 2 points)


def test_recursive_geohash_does_nothing():

    shp_bug = gpd.GeoDataFrame(
            {
            'id' :[0	,1,	2	,3],
            'geohash_index' : ["drgp",	"drgr",	"f0r9",	"f0rb"],  #also tests name colisions for `geohash_index`
            "lat" : [44.997203	,44.996874	,46.722120	,46.521345],
            "lng": [-74.349024,	-74.084504,	-79.103772	,-78.868221],
            'counts' : [134, 410, 119	,59],
            'geometry': [Point(-74.34902, 44.99720),Point(-74.08450,44.99687),Point(-79.10377,46.72212),Point(-78.86822,46.52135)]
            },
            crs = 4326          
    )
        
    shp_part, _ = recursively_partition_geohash_cells(shp_bug,
                                                        count_column_name='counts',
                                                        min_num_points = 2)

    assert shp_part[shp_part['counts'] > 0].shape[0] == shp_bug.shape[0] # same number of non-zero rows: we tried drilling down but cannot go any more recise than the 4 individual points



if __name__ == '__main__':

    test_recursive_geohash_refines()
 