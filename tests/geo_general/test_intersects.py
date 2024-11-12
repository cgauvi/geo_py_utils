
import geopandas as gpd
import numpy as np
import pytest

from geo_py_utils.geo_general.intersection import intersect_gdf
from geo_py_utils.utilities_testing.data import (
    GDF_SQUARE_OVER_GRID, 
    GDF_GRID, 
    GDF_EMPTY,
    GDF_POINT_OVER_SQUARE_VERTEX,
    GDF_LINE_OVER_SQUARE_EDGE
)



def test_intersect_creates_expected_number_polygons_same_index_name():

    gdf_inter = intersect_gdf(GDF_SQUARE_OVER_GRID, 'index' , GDF_GRID, 'index'  )

    assert gdf_inter.shape[0] == 4, 'Fatal error! intersecting the slightly shifted square with a grid of 4 polygons should yield 6 rows'

    assert np.isin(np.array(['index_left','index_right']),gdf_inter.columns).all(), 'Fatal error! same index: left and right suffixes added '

def test_intersect_creates_same_results_regardless_flipped_order():

    gdf_inter_1 = intersect_gdf(GDF_SQUARE_OVER_GRID, 'index' , GDF_GRID, 'index'  )
    gdf_inter_2 = intersect_gdf(GDF_GRID , 'index' , GDF_SQUARE_OVER_GRID, 'index'  )

    assert gdf_inter_1.shape[0] == gdf_inter_2.shape[0], ' Order should not influence result of intersection'

    gdf_merged = gdf_inter_1.merge(
        gdf_inter_2, 
        left_on=['index_left','index_right'],
        right_on=['index_right', 'index_left']
    )


    gdf_merged['area_1']= gpd.GeoSeries(gdf_merged.geometry_x).area
    gdf_merged['area_2']= gpd.GeoSeries(gdf_merged.geometry_y).area

    assert (gdf_merged['area_1'] == gdf_merged['area_2']).all(), ' Order should not influence result of intersection'

def test_intersect_fails_with_incoherent_proj():

    with pytest.raises(ValueError):
        intersect_gdf(GDF_SQUARE_OVER_GRID.to_crs(3347), 'index' , GDF_GRID.to_crs(3857), 'index'  )



def test_intersect_fails_with_zero_rows():

    with pytest.raises(ValueError):
        intersect_gdf(GDF_SQUARE_OVER_GRID.to_crs(3347), 'index' , GDF_EMPTY.to_crs(3347), 'index'  )

    


def test_intersect_yields_lower_dim_point():

 
    gdf_merged = intersect_gdf(GDF_SQUARE_OVER_GRID , 'index' , GDF_POINT_OVER_SQUARE_VERTEX , 'index'  )

    assert gdf_merged.shape[0] == 1
    assert gdf_merged.geom_type.values[0] == 'Point'



def test_intersect_yields_lower_dim_line():

 
    gdf_merged = intersect_gdf(GDF_SQUARE_OVER_GRID , 'index' , GDF_LINE_OVER_SQUARE_EDGE , 'index'  )

    assert gdf_merged.shape[0] == 1
    assert gdf_merged.geom_type.values[0] == 'LineString'
# %%

if __name__ == '__main__':
    test_intersect_creates_same_results_regardless_flipped_order()