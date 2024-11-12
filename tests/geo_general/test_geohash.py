import geopandas as gpd
import geohash
import numpy as np
import pytest
from shapely.geometry import Polygon, Point, LineString
from shapely import GeometryCollection

from geo_py_utils.geo_general.bbox import get_list_coordinates_from_bbox
from geo_py_utils.geo_general.geo_utils import get_geodataframe_from_list_coord
from geo_py_utils.geo_general.geohash_utils import (
    get_geohash_box,
    get_all_geohash_from_gdf,
    geohash_max_precision,
    recursively_partition_geohash_cells, 
    recursively_partition_polygons_geohash_cells,
    filter_by_num_points,
    get_num_points_in_geoseries
)
from geo_py_utils.utilities_testing.data import US_ALL_CVX_GDF, GDF_BBOX_DONNACONNA

EPS = 10**-6

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
    shp_us = US_ALL_CVX_GDF

    # Max precision should be 1 and should get a warning since no single geohash cell is large enough to cover the entire region
    max_prec_us = geohash_max_precision(shp_us)
 
    assert max_prec_us == 1
    


def test_recursive_geohash_refines():

    max_prec = 4
    min_num_points = 1

    shp_all_1 = gpd.GeoDataFrame(
        {
            'id': [0,1 ,2],
            "geometry": [Point (-71.16451, 46.86968),   Point (-71.25148, 48.41987), Point (-71.25148, 48.419)	], #slight tweak - same geohash 
            "counts": [1.0, 1.0, 1.0]
    }   ,
    crs = 4326
    )

    shp_part, dict_shps = recursively_partition_geohash_cells(shp_all_1,
                                                    count_column_name='counts',
                                                    min_num_points=min_num_points,
                                                    max_precision=max_prec)

    # aggregated 2 geohashes into one (last 2 points)
    assert shp_part[shp_part['counts'] > 0].shape[0] == shp_all_1.shape[0]-1 
    assert max(dict_shps.keys()) <= max_prec

    # Make sure the cells that are suff refined (low geohash prec) respect the num of points constraint
    for k in dict_shps.keys():
        if k < max_prec:
            dict_shps[k].counts.max() <= min_num_points

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
        
    shp_part, _ = recursively_partition_geohash_cells(
        shp_bug,
        count_column_name='counts',
        min_num_points = 2
    )

    assert shp_part[shp_part['counts'] > 0].shape[0] == shp_bug.shape[0] # same number of non-zero rows: we tried drilling down but cannot go any more recise than the 4 individual points


def test_recursive_area_partition_does_nothing_if_thresh_too_high():
 
    # GDF_BBOX_DONNACONNA.to_crs(3347).area 1.305043e+09

    gdf_untouched = recursively_partition_polygons_geohash_cells(
        GDF_BBOX_DONNACONNA,
        gdf_id='index',
        max_area = GDF_BBOX_DONNACONNA.to_crs(3347).area + EPS
    )

    assert gdf_untouched.shape[0] == GDF_BBOX_DONNACONNA.shape[0]


def test_recursive_polyon_geohash_parttion_fails_on_non_geom():

    with pytest.raises(Exception):
        gdf_point = gpd.GeoDataFrame(
             { 'id': [0,],  "geometry": [Point (-71.16451, 46.86968)] }   ,
             crs = 4326
             )
         
        recursively_partition_polygons_geohash_cells(gdf_point, gdf_id='id')

    with pytest.raises(Exception):
        gdf_line = gpd.GeoDataFrame(
             { 'id': [0,],  "geometry": [LineString( [(-71.16451, 46.86968), (-71.16451, 46.86968)])] }   ,
             crs = 4326
             )
        recursively_partition_polygons_geohash_cells(gdf_line, gdf_id='id')
        
    with pytest.raises(Exception):
        gdf_geom_collection = gpd.GeoDataFrame(
             { 
                 'id': [0,],  
                 "geometry": [
                     GeometryCollection( 
                        [
                            LineString( [(-71.16451, 46.86968),(-72.16451, 47.86968)]),
                            Polygon(((-71.5, 46.5), (-71.6, 46.4),  (-71.5, 46.7),   (-71.5, 46.5))) 
                        ]
                    )
                 ] 
            }   ,
            crs = 4326
            )
        recursively_partition_polygons_geohash_cells(gdf_geom_collection, gdf_id='id')
         




def test_recursive_area_partition_geohash_max_prec_respected_no_issue_idx_collision():
    # GDF_BBOX_DONNACONNA.to_crs(3347).area 
    # 1.305043e+09
    # c.f. 
    # get_all_geohash_from_geohash_indices(['f2kbz']).to_crs(3347).area # geohash 5
    # 1.679750e+07

    max_area = float(GDF_BBOX_DONNACONNA.to_crs(3347).area - EPS) # to ensure we will subdivide the gdf at least once

    gdf_broken_down_gh_5 = recursively_partition_polygons_geohash_cells(
        GDF_BBOX_DONNACONNA.rename(columns={'index':"geohash_index"}),
        init_geohash_prec = 4,
        max_geohash_prec = 5,
        gdf_id='geohash_index',
        max_area = max_area
    )

    #index names
    assert np.isin(np.array(['geohash_index_right', 'geohash_index_left']), gdf_broken_down_gh_5.columns).all(), 'Fatal error! should have a collision in column names'


    gdf_broken_down_gh_5_broken_down = gdf_broken_down_gh_5[~gdf_broken_down_gh_5['geohash_index_left'].isna()]
    gdf_broken_down_gh_5_same = gdf_broken_down_gh_5[gdf_broken_down_gh_5['geohash_index_left'].isna()]

    # areas
    assert (gdf_broken_down_gh_5_broken_down['geohash_index_left'].str.len() <= 5).all(), 'Fatal error! max geohash prec is 5'
    if gdf_broken_down_gh_5_same.shape[0]:
        assert (gdf_broken_down_gh_5_same.area <= max_area).all(), 'Fatal error! geom should not have been browken down'

 

def test_recursive_polygon_area_partition_geohash_only_polygons_and_expected_cells ():

    max_area = float(GDF_BBOX_DONNACONNA.to_crs(3347).area - EPS) # to ensure we will subdivide the gdf at least once

    gdf_broken_down_gh_4 = recursively_partition_polygons_geohash_cells(
        GDF_BBOX_DONNACONNA,
        init_geohash_prec = 4,
        max_geohash_prec = 4,
        gdf_id='index',
        max_area = max_area
    )

    df_expected_geohash = get_all_geohash_from_gdf(GDF_BBOX_DONNACONNA, 4, 'intersects')

    assert gdf_broken_down_gh_4.geom_type.isin(['Polygon']).all()
    assert gdf_broken_down_gh_4.shape[0] == df_expected_geohash.shape[0], 'Fatal error! there should be 5 intersecting geohashes '
    assert gdf_broken_down_gh_4['geohash_index'].isin(df_expected_geohash['geohash_index']).all()


def test_recursive_polygon_num_points_partition_geohash_give_exp_number_polygons():


    # 16 points by quarter circle: 16*4+1 = 65 point in total
    # If we intersect the circle with the 4 level 4 geohash that intersect it, we should get 4 polygons, each wich 16 + 4 = 18 points instead
    # (Assuming we split the circle perfectly in the middle - which is not the case here)
    geo_series_buffer = GDF_BBOX_DONNACONNA.centroid.buffer(0.1,resolution=16)
    gdf_broken_down_gh_4=recursively_partition_polygons_geohash_cells(
        gdf = gpd.GeoDataFrame({'index':[0]}, geometry=geo_series_buffer, crs=4326),
        gdf_id='index',
        init_geohash_prec = 4,
        max_geohash_prec = 4,
        filter_fun=filter_by_num_points,
        max_num_points = float(get_num_points_in_geoseries(geo_series_buffer) - 1)
    )

    # 4 polyons
    assert gdf_broken_down_gh_4.shape[0] == 4

    #We cannot remove any points without loosing precision
    assert get_num_points_in_geoseries(gdf_broken_down_gh_4.geometry).sum() >= float(get_num_points_in_geoseries(geo_series_buffer))
    
    # however, each polygon has less than the threshold
    assert get_num_points_in_geoseries(gdf_broken_down_gh_4.geometry).max() <  float(get_num_points_in_geoseries(geo_series_buffer) - 1)


def test_recursive_polygon_num_points_partition_geohash_multi_precision_not_overly_subdivided():
        
        geo_series_buffer = GDF_BBOX_DONNACONNA.centroid.buffer(0.1,resolution=16)
        gdf_buffer = gpd.GeoDataFrame({'index':[0]}, geometry=geo_series_buffer, crs=4326)

        gdf_broken_down_gh_4=recursively_partition_polygons_geohash_cells(
            gdf = gdf_buffer,
            gdf_id='index',
            init_geohash_prec = 4,
            max_geohash_prec = 6,
            filter_fun=filter_by_num_points,
            max_num_points =  5 # a valid rectangle Polygon has 5 points since beginning and end are repeated
        )
        gdf_broken_down_gh_5 = gdf_broken_down_gh_4[gdf_broken_down_gh_4['geohash_index'].str.len()==5]

        gdf_geohash_level_5_4_points = get_all_geohash_from_gdf(gdf_buffer, 5, 'strictly within')

        # gdf_geohash_level_5_4_points is a subset (in fact strict) of gdf_broken_down_gh_5
        assert (
            gdf_geohash_level_5_4_points['geohash_index'].isin(gdf_broken_down_gh_5['geohash_index']).all()
        ),'''
        Fatal error! The geohash 5 that are strictly within the original circle/buffer should not be further subdivided
        indeed they all have 4 points < max_num_points
        '''

        


def test_geohash_from_gdf_works_all_predicates():
    
    geo_series_buffer = GDF_BBOX_DONNACONNA.centroid.buffer(0.1,resolution=16)
    gdf_buffer = gpd.GeoDataFrame({'index':[0]}, geometry=geo_series_buffer, crs=4326)

    # 25 geohash cells of prec 5 intersecting the circle/buffer
    list_gh_5_intersecting_buffer = [  "f2kbq", "f2m12","f2m11","f2m10","f2m0c","f2m0b","f2m09","f2m08","f2m03","f2m02","f2kcr","f2kcq","f2kcp","f2kcn","f2kcm", "f2kcj","f2kbz","f2kby","f2kbx","f2kbw","f2kbv","f2kbt","f2kbr","f2kbp","f2kbm"]
    list_extra_gh_5_intersecting_buffer_bbox = ["f2m13", "f2m01", "f2m00", "f2kbj" ,"f2kbn"]
    list_gh_5_intersecting_buffer_bbox = list_gh_5_intersecting_buffer.copy()
    list_gh_5_intersecting_buffer_bbox.extend(list_extra_gh_5_intersecting_buffer_bbox)

    assert len(set(list_gh_5_intersecting_buffer)) == 25
    
    list_gh_5_within_buffer = ["f2kbz","f2kby","f2kcp","f2kcn","f2m0b","f2kbx","f2m08","f2kbw"]
    #list_gh_5_strictly_within_buffer == list_gh_5_within_buffer
    list_gh_5_strictly_bbox_12= [  "f2kbq", "f2kbr" ,"f2m02", "f2m10", "f2m0b","f2m08","f2kcp","f2kcn","f2kbz","f2kby","f2kbx","f2kbw"]
    #list_gh_5_strictly_within_bbox_12 == list_gh_5_within_bbox_12
    list_gh_4_contain_strictly  = ['f2kb']
    list_gh_extra_contain = ['f2kc', 'f2m1' , 'f2m0' ]
    list_gh_4_contain_all = list_gh_4_contain_strictly.copy()
    list_gh_4_contain_all.extend(list_gh_extra_contain)
    list_gh_5_strict_within_bbox = ['f2kbz', 'f2kbx']

    assert len(set(list_gh_5_within_buffer)) == 8
    assert len(set(list_gh_5_strictly_bbox_12)) == 12

    # within - test case where only edges overlap
    gdf_buffer_bbox = gpd.GeoDataFrame(
        {'index': [0]},
        geometry = gdf_buffer.envelope, # gdf_buffer.unary_union.envelope is POLYGON not geoseries
        crs=4326
    )
    gdf_12_within_buffer_bbox = get_all_geohash_from_gdf(gdf_buffer_bbox, 5, 'within')
    assert (
        gdf_12_within_buffer_bbox['geohash_index'].isin(list_gh_5_strictly_bbox_12).all() and 
        np.isin(list_gh_5_strictly_bbox_12, gdf_12_within_buffer_bbox['geohash_index']).all()
    ), 'Fatal error with within'


    # strictly within - same results as within since edge don't touch
    gdf_12_strict_within_buffer_bbox = gpd.GeoDataFrame(
        {'geohash_index': list_gh_5_strictly_bbox_12},
        geometry = [Polygon(get_geohash_box(h)) for h in list_gh_5_strictly_bbox_12], # gdf_12_strict_within_buffer_bbox.unary_union.envelope is POLYGON not geoseries
        crs=4326
    )

    gdf_gh_bbox = gpd.GeoDataFrame(
        {'index': [0]},
        geometry = [gdf_12_within_buffer_bbox.unary_union.envelope], # gdf_buffer.unary_union.envelope is POLYGON not geoseries
        crs=4326
    )

    # strictly within vs within - diff results since edge touch
    gdf_gh_bbox_gh_5_within = get_all_geohash_from_gdf(gdf_gh_bbox, 5, 'within')
    gdf_gh_bbox_gh_5_strict_within = get_all_geohash_from_gdf(gdf_gh_bbox, 5, 'strictly within')
 
    assert (
        (gdf_gh_bbox_gh_5_within.shape[0] == gdf_12_within_buffer_bbox.shape[0]) and
        (gdf_gh_bbox_gh_5_strict_within.shape[0] == gdf_12_within_buffer_bbox.shape[0] - 10) and 
        (gdf_gh_bbox_gh_5_strict_within['geohash_index'].isin(list_gh_5_strict_within_bbox)).all()
    ), 'Fatal error with strictly within'

    # within - tests case where gdf_buffer is not the same as gdf_buffer.envelope (bounding box)
    df_expected_geohash_within = get_all_geohash_from_gdf(gdf_buffer, 5, 'within')
    assert (
        df_expected_geohash_within['geohash_index'].isin(list_gh_5_within_buffer).all() and 
        np.isin(list_gh_5_within_buffer, df_expected_geohash_within['geohash_index']).all()
    ), 'Fatal error with within'


    # intersects - also tests case where gdf_buffer is not the same as gdf_buffer.envelope  
    # df_expected_geohash_intersect.shape[0] == 30 (5*6) if using the enveloppe 
    df_expected_geohash_intersect_env = get_all_geohash_from_gdf(gdf_buffer_bbox, 5, 'intersects')
    df_expected_geohash_intersect = get_all_geohash_from_gdf(gdf_buffer, 5, 'intersects')

    assert df_expected_geohash_intersect_env.shape[0] == 30
    assert df_expected_geohash_intersect.shape[0] == df_expected_geohash_intersect_env.shape[0] - 5, 'Fatal, there are 5 cells that intersect the bbox, but not the actual polygon'
    assert (
        df_expected_geohash_intersect['geohash_index'].isin(list_gh_5_intersecting_buffer).all() and 
        np.isin(list_gh_5_intersecting_buffer, df_expected_geohash_intersect['geohash_index']).all()
    ), 'Fatal error with intersect'


    # contains - list_gh_4_contain_all - all the geohash 4 parents of the list_gh_5_strictly_bbox_12
 
    df_expected_contains = get_all_geohash_from_gdf(gdf_12_strict_within_buffer_bbox, 4, 'contains', any)
    assert (
        df_expected_contains['geohash_index'].isin(list_gh_4_contain_all).all() and 
        np.isin(list_gh_4_contain_all, df_expected_contains['geohash_index']).all()
    ), 'Fatal error with contains'
 