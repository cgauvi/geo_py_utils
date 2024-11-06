import geopandas as gpd
import pytest
 
from geo_py_utils.geo_general.valid_geom import make_extra_valid_gpd, make_valid_gpd
from geo_py_utils.utilities_testing.data import LIST_INVALID_GDF, GDF_SLIVER, SAMPLE_GDF, SAMPLE_GDF_2, GDF_INVALID_SELF_INTERSECTING
from geo_py_utils.geo_general.geo_utils import explode_multiparts

def test_basic_make_valid_works_on_valid():

    # each geometry/row in each gdf should be valid
    # this should work without setting the crs
    for g in [SAMPLE_GDF , SAMPLE_GDF_2]:
        orig_invalid = g.is_valid.all()
        fixed_valid = make_valid_gpd(g).is_valid.all()
        assert orig_invalid and fixed_valid 


def test_both_valid_work_on_any_geom_col_name():

    # each geometry/row in each gdf should be valid
    # this should work without setting the crs
    make_valid_gpd(GDF_INVALID_SELF_INTERSECTING.rename_geometry('bla')).is_valid.all()
    make_extra_valid_gpd(GDF_INVALID_SELF_INTERSECTING.set_crs(3857).rename_geometry('bla')).is_valid.all()


def test_basic_make_valid_works_on_invalid():

    # each geometry/row in each gdf should be valid
    # this should work without setting the crs
    for g in LIST_INVALID_GDF:
        orig_invalid = ~g.is_valid.all()
        fixed_valid = make_valid_gpd(g).is_valid.all()
        assert orig_invalid and fixed_valid 


def test_extra_make_valid_fails_when_no_crs():

    with pytest.raises(Exception):
        make_extra_valid_gpd(GDF_SLIVER)


def test_extra_make_valid_defaults():

    gdf_extra_trans =  make_extra_valid_gpd(GDF_SLIVER.set_crs(3857))

    assert (
        gdf_extra_trans.is_valid.all() and 
        GDF_SLIVER.is_valid .all()
    )

    assert gdf_extra_trans.shape[0] == 0 , 'Fatal error! we should have removed all rows!'



def test_extra_make_valid_defaults_works_on_invalid_with_crs():

    gdf_extra_trans =  make_extra_valid_gpd(GDF_SLIVER.set_crs(3857))

    assert (
        gdf_extra_trans.is_valid.all() and 
        GDF_SLIVER.is_valid .all()
    )

    assert gdf_extra_trans.shape[0] == 0 , 'Fatal error! we should have removed all rows!'



def test_make_valid_explosion_splits_into_expected_num_rows():

    gdf_valid_multipolygon = make_valid_gpd(GDF_INVALID_SELF_INTERSECTING)

    assert gdf_valid_multipolygon.shape[0] == 1, 'Fatal error, validation should create multipolygon from invalid polygon'

    gdf_valid_polygon = explode_multiparts(gdf_valid_multipolygon)

    assert gdf_valid_polygon.shape[0] == gdf_valid_multipolygon.shape[0] + 1, 'Fatal error, explode_multiparts should create 2 lines from initial multipolygon'



def test_extra_make_valid_produces_zero_rows_if_filter_too_aggressive():

    gdf_extra_trans =  make_extra_valid_gpd(SAMPLE_GDF.to_crs(3857), thresh_area_m=10**10)

    assert gdf_extra_trans.shape[0] ==  0, 'Should have filtered ewverything'

def test_extra_make_valid_does_nothing_if_filter_too_conservative():
    gdf_extra_trans =  make_extra_valid_gpd(GDF_SLIVER.set_crs(3857), sliver_ratio_thres = 10**10, thresh_area_m=None)

    assert gdf_extra_trans.shape[0] == GDF_SLIVER.shape[0], 'Should not have filtered anything'