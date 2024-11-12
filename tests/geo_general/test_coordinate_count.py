import numpy as np

from geo_py_utils.geo_general.geo_utils import get_num_points_in_geoseries
from geo_py_utils.utilities_testing.data import (
    GDF_GEOM_COLLECTION,
    GDF_EMPTY,
    GDF_GEOM_EXPLODED
)


def test_count_same_geocollection_or_exploded():

    num_coordinates_exploded = np.sum(get_num_points_in_geoseries(GDF_GEOM_COLLECTION.geometry.explode()))
    num_coordinates_raw = get_num_points_in_geoseries(GDF_GEOM_COLLECTION.geometry)

    assert len(num_coordinates_raw) == 1
    assert num_coordinates_exploded == num_coordinates_raw[0]


def test_count_empty_for_empty_geom():
    assert len(get_num_points_in_geoseries(GDF_EMPTY.geometry)) == 0


def test_count_works_all_geom_types():
    GDF_GEOM_EXPLODED['actual_coord_count'] = get_num_points_in_geoseries(GDF_GEOM_EXPLODED.geometry)

    assert (
        GDF_GEOM_EXPLODED['actual_coord_count'] == GDF_GEOM_EXPLODED ['expected_coord_count']
    ).all() 