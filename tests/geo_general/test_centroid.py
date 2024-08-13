import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from geo_py_utils.geo_general.centroid import get_centroid_gpd 

BASIC_POLY_NO_CRS = gpd.GeoDataFrame(
    {'id': [0]},
    geometry=[
        Polygon(((-71.5, 46.5),
                (-71.6, 46.4),
                (-71.5, 46.7),
                (-71.5, 46.5)))
                ],
    crs=None
)

BASIC_POLY_4326 = BASIC_POLY_NO_CRS.set_crs(4326)



@pytest.mark.parametrize('shp', [BASIC_POLY_NO_CRS, BASIC_POLY_4326])
def test_get_centroid_gpd(shp):

    centroid = get_centroid_gpd(shp)
    assert centroid.crs == shp.crs