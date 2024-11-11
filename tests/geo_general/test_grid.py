import pytest

from geo_py_utils.geo_general.grid import get_grid_over_shp
from geo_py_utils.utilities_testing.data import SAMPLE_GDF_2

EPS=10**-6

def test_grid_fails_cell_dim_or_height_width_specified():

    with pytest.raises(Exception):
        get_grid_over_shp(SAMPLE_GDF_2)


def test_grid_covers_gdf():

    shp_grid = get_grid_over_shp(SAMPLE_GDF_2, cell_dim =(100,100) )

    assert shp_grid.unary_union.envelope.covers(SAMPLE_GDF_2.unary_union), 'Fatal error! should cover the entire gdf!'



def test_grid_same_correct_crs():

    shp_grid = get_grid_over_shp(SAMPLE_GDF_2, cell_dim =(100,100) )

    assert shp_grid.crs == SAMPLE_GDF_2.crs, 'Fatal error! should cover the entire gdf!'

    shp_grid = get_grid_over_shp(SAMPLE_GDF_2, height_y_m = 500, width_x_m= 500, crs_to_use_for_grid=3347 )

    assert shp_grid.crs == SAMPLE_GDF_2.crs, 'Fatal error! should cover the entire gdf!'



def test_grid_too_coarse_fails():

    shp_proj = SAMPLE_GDF_2.to_crs(3347)

    minx, miny, maxx, maxy = shp_proj.total_bounds

    max_height = maxy-miny
    max_width = maxx-minx

    with pytest.raises(ValueError):
        get_grid_over_shp(shp_proj,  height_y_m = max_height+EPS, width_x_m= max_width-EPS)

    with pytest.raises(ValueError):
        get_grid_over_shp(shp_proj,  height_y_m = max_height-EPS, width_x_m= max_width+EPS)

    
    assert get_grid_over_shp(shp_proj,  height_y_m = max_height-EPS, width_x_m= max_width-EPS).shape[0] > 0


if __name__ == '__main__':

    test_grid_too_coarse_fails()