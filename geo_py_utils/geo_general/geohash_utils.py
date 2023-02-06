

import geohash
from warnings import warn
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape, Polygon
from typing import Union, List
from os import environ
from os.path import join
import numpy as np


from geo_py_utils.geo_general.geo_utils import get_geodataframe_from_list_coord, get_matrix_point_coordinates, get_matrix_point_polygon
from geo_py_utils.geo_general.centroid import add_centroid
from geo_py_utils.misc.constants import ROOT_DIR
from geo_py_utils.data.datasets import get_geohash_worst_dim_path



import logging

logger = logging.getLogger(__name__)


PATH_GEO_HASH_REFERENCE = get_geohash_worst_dim_path()


def count_by_index(df,
                   group_by_idx='geohash_index',
                   agg_dict={'howMany': np.sum}) -> pd.DataFrame:

    """Extremelly basic convenience function that groups by index and aggregates variables using a dict

    Args:
        group_by_idx : grouping var (ex: geohash_index, GEOID, etc)
        agg_dict: agregation mapping (ex:{'howMany': np.sum})
    Returns:
        df_count_by[pd.DataFrame]: aggregated dataframe
    """

    df_count_by = df.\
        groupby(group_by_idx).\
        agg(agg_dict).\
        reset_index()

    return df_count_by



def geohash_max_precision(shp: gpd.GeoDataFrame,
                          crs=None,
                          path_reference=PATH_GEO_HASH_REFERENCE):

    """Get the largest (most precise) geohash precision such that a single cell completely contains the geodataframe's extent

    This means that 1 level BELOW that value, we will get at least 2 cells covering that extent

    Args:
        shp (gpd.GeoDataFrame): _description_
        crs (_type_, optional): _description_. Defaults to None.
        path_reference (_type_, optional): _description_. Defaults to PATH_GEO_HASH_REFERENCE.

    Returns:
        int: _description_
    """
    if crs is None:
        shp = shp.to_crs(3857)

    west, south, east, north = shp.total_bounds

    width = (east-west)
    height = (north - south)

    # This hard coded table is taken from:https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-geohashgrid-aggregation.html
    # Geohashes are based on lat, lng and so their width and height changes for the same precision based on the location on earth
    # The values in the table are the 'worst' ie the largest the cell can be
    # If width > df_reference.Width and height > df_reference.height -> width > true cell width and height > true cell height everywhere and we need only a single cell for that extent
    df_reference = pd.read_csv(path_reference)

    # Both the width and height refernce need to be >=
    idx_all_larger = (df_reference.Width >= width) & (
        df_reference.Height >= height)
    idx_prec = max(idx_all_larger.idxmin() - 1, 0)
    prec = df_reference.Precision[idx_prec]

    # The best case scenario (smallest geohash cell - most precise) is when
    # 1) the geohash cell has the exact width and heiht of the bounding box
    # 2) the extents completely overlap
    #
    # However, we can have geohash cells that are much larger than the bounding box such that no single one completely contains the smaller extent
    #   --> depends on the alignment of geohash cells and extent
    #
    # So start with the first potential allowable geohash size and go up until all extent points are contained

    unique_hashes = get_all_geohash_index_from_extent(
        shp.to_crs(4326).total_bounds, precision=prec)
    is_geohash_cell_unique = len(unique_hashes) == 1

    while not is_geohash_cell_unique and prec >= 1:
        if prec == 1:
            break
        prec = prec - 1

        unique_hashes = get_all_geohash_index_from_extent(
            shp.to_crs(4326).total_bounds, precision=prec)
        is_geohash_cell_unique = len(unique_hashes) == 1

    if prec == 1:
        logger.warning(
            'Warning! Reached max geohahsh precision of 1 and the bounding box is still does not contained within a single geohash cell!')

    return prec


 
 
def get_geohash_box(current_geohash, lng_first=True):
    """Returns a list of 4 2D coordinates corresponding to a given geohash cell

    Convenience wrapper over geohash.bbox

    Args:
        current_geohash (_type_): _description_
        lng_first (bool, optional): _description_. Defaults to True.

    Returns:
        a list representation of the polygon
    """

    b = geohash.bbox(current_geohash)

    # By default return the lng first and lat second
    polygon = [[b['w'], b['s']],
               [b['w'], b['n']],
               [b['e'], b['n']],
               [b['e'], b['s']]]

    # Swap coordinates
    if not lng_first:
        polygon = [[p[1], p[0]] for p in polygon]

  # polygon: list of list of points
    return polygon


def is_geohash_in_bounding_box(current_geohash,
                               bbox_coordinates,
                               lng_first=True,
                               predicate='intersects'):
    """Checks if the box of a geohash is inside the bounding box

    Not 100% sure what happens if we change the < to <= 
    Is it possible to get a cell that only touches one of the edges of the overal boundin box? 

    :param current_geohash: a geohash
    :param bbox_coordinates: bounding box coordinates
    :param predicate: Can the geohash cell touch the bounding box> or must it lie stricty within?
    :return: true if the the center of the geohash is in the bounding box
    """

    assert predicate in ['within', 'strictly within', 'intersects', 'contains']

    # List of 4 elements. Each is a 2 dim list representing a point
    coordinates = get_geohash_box(current_geohash, lng_first)

    min_lng, max_lng = bbox_coordinates[0], bbox_coordinates[2]
    min_lat, max_lat = bbox_coordinates[1], bbox_coordinates[3]

    # For each of the 4 geohash bounding box points, check if any are within the bbox
    point_within = [((min_lng <= coordinates[k][0]) and (coordinates[k][0] <= max_lng))
                    and ((min_lat <= coordinates[k][1]) and (coordinates[k][1] <= max_lat))
                    for k in range(len(bbox_coordinates))]
    # ... (or strictly within)
    point_within_strict = [((min_lng < coordinates[k][0]) and (coordinates[k][0] < max_lng))
                           and ((min_lat < coordinates[k][1]) and (coordinates[k][1] < max_lat))
                           for k in range(len(bbox_coordinates))]

    # Does the geohash cell completely contain the bbox?
    min_lng_cell = np.min([coordinates[k][0]
                          for k in range(len(bbox_coordinates))])
    max_lng_cell = np.max([coordinates[k][0]
                          for k in range(len(bbox_coordinates))])
    min_lat_cell = np.min([coordinates[k][1]
                          for k in range(len(bbox_coordinates))])
    max_lat_cell = np.max([coordinates[k][1]
                          for k in range(len(bbox_coordinates))])

    # Does the bounding box contain (not necessarily properly) the bbox
    contains = (min_lng_cell <= min_lng) and (max_lng <= max_lng_cell)  \
        and (min_lat_cell <= min_lat) and (max_lat <= max_lat_cell)

    # Intersection: ie not disjoint
    # See https://gamedev.stackexchange.com/questions/586/what-is-the-fastest-way-to-work-out-2d-bounding-box-intersection
    # If testing for intersection, we want at least 1 point in common: this also means the geohash cell can CONTAIN the bbox
    intersects = not(min_lng_cell > max_lng
                     or max_lng_cell < min_lng
                     or max_lat_cell < min_lat
                     or min_lat_cell > max_lat)

    # Now look at each of the 4 points
    if predicate == 'strictly within':
        spatial_bool = np.all(point_within_strict)
    elif predicate == 'within':
        spatial_bool = np.all(point_within)
    elif predicate == 'intersects':
        spatial_bool = intersects
    else:
        spatial_bool = np.all(contains)

    return spatial_bool


def get_all_geohash_index_from_extent(bbox_coordinates, precision, lng_first=True, predicate='intersects'):
    """Computes all geohash tile in the given bounding box

    Taken from https://blog.tafkas.net/2018/09/28/creating-a-grid-based-on-geohashes/

    Args:
        bbox_coordinates : list/tuple of list/tuple representing bounding box / extent
        precision: geohash precision
    Returns:
        list[str]: list of geohash indices
    """

    checked_geohashes = set()
    geohash_stack = set()
    geohashes = []

    # get center of bounding box, assuming the earth is flat ;)
    if not lng_first:
        center_latitude = (bbox_coordinates[0] + bbox_coordinates[2]) / 2
        center_longitude = (bbox_coordinates[1] + bbox_coordinates[3]) / 2
    else:
        center_longitude = (bbox_coordinates[0] + bbox_coordinates[2]) / 2
        center_latitude = (bbox_coordinates[1] + bbox_coordinates[3]) / 2

    center_geohash = geohash.encode(
        center_latitude, center_longitude, precision=precision)

    # Make sure the initial geohash cell is within the bounding box -> method wont return anything if this fails.
    if is_geohash_in_bounding_box(center_geohash, bbox_coordinates, lng_first=lng_first, predicate=predicate):
        geohashes.append(center_geohash)
    else:
        print(f'Warning! there are no geohash cells {predicate} for precision {precision}! \
            Try a higher precision or change the predicate to intersects for instance')

    geohash_stack.add(center_geohash)
    checked_geohashes.add(center_geohash)

    while len(geohash_stack) > 0:
        current_geohash = geohash_stack.pop()
        neighbors = geohash.neighbors(current_geohash)
        for neighbor in neighbors:
            if neighbor not in checked_geohashes and is_geohash_in_bounding_box(neighbor, bbox_coordinates, lng_first=lng_first, predicate=predicate):
                geohashes.append(neighbor)
                geohash_stack.add(neighbor)
                checked_geohashes.add(neighbor)

    return geohashes


def get_all_geohash_from_geohash_indices(geohash_indices: List) -> gpd.GeoDataFrame:
    """Simple convenience function that return a single geodataframe containing all geohashes cells
    from a list of geohash indices

    Args:
        indices : list of geohash indicese.g. ['f2k','f2m']

    Returns:
         shp_geohash (gpd.GeoDataFrame): geodf with geohash cells
    """

    # Make sure valid list and not some iterable with a 'len' implemented like a str
    assert len(geohash_indices) > 0
    assert isinstance(geohash_indices, tuple) or isinstance(
        geohash_indices, list)

    geohash_bbox_coordinates = [get_geohash_box(
        gh_index) for gh_index in geohash_indices]
    geo_shp = get_geodataframe_from_list_coord(
        geohash_bbox_coordinates, crs=4326)

    return (geo_shp)


def get_all_geohash_from_extent(input: Union[List, gpd.GeoDataFrame, gpd.geoseries.GeoSeries],
                                precision: int,
                                crs=None,
                                lng_first=True,
                                predicate='intersects') -> gpd.GeoDataFrame:
    """Return a single geodataframe containing all geohashes from a geodataframe or the geodataframe's extent by using the extent

    Args:
        bbox_coordinates : list/tuple of list/tuple representing bounding box / extent
        precision: geohash precision
    Returns:
         shp_geohash (gpd.GeoDataFrame): geodf with geohash indices
    """

    if isinstance(input, gpd.GeoDataFrame) or isinstance(input, gpd.geoseries.GeoSeries):
        bbox = input.total_bounds
        crs = input.crs
    elif pd.api.types.is_list_like(input):
        if len(input) != 4:
            raise ValueError('get_all_geohash_from_extent accepts a bounding box with \
                            4 elements representing the min and max values for both dimensions')
        if crs is None:
            raise ValueError(
                'get_all_geohash_from_extent requires a crs if sending a list')
        bbox = input
    else:
        raise ValueError(
            'get_all_geohash_from_extent accepts either a geodataframe or a  ')

    # These are unique since we use a set
    geohash_indices = get_all_geohash_index_from_extent(
        bbox, precision=precision, lng_first=lng_first, predicate=predicate)

    list_geohash_bbox = [get_geohash_box(
        g, lng_first=lng_first) for g in geohash_indices]
    shp_geohash = get_geodataframe_from_list_coord(list_geohash_bbox, crs)
    shp_geohash['geohash_index'] = geohash_indices

    return shp_geohash


def get_all_geohash_from_gdf(input:  gpd.GeoDataFrame,
                             precision: int,
                             predicate: str = 'within'):
    """Return a single geodataframe containing ALL geohashes from a geodataframe 

    Wrapper over get_all_geohash_from_extent 

    Different feature from get_all_geohash_from_extent which returns all geohash inside a bbox
    For this function, we return only the geohash cell over points/simple feature centroids that are in our input geodataframe

    Args:
        input : geo df -> each simple feature gets mapped to a geohash (and then we take )
        precision: geohash precision
    Returns:
        list[str]: list of geohash indices
    """

    assert isinstance(input, gpd.GeoDataFrame)

    # Get the bounding box over EACH feature - NOT total_bounds
    matrix_bbox = input.bounds.to_numpy()

    # Use the bounding box for EACH feature to get all geohash in that extent + then concat all gdf
    shp_geo_all = pd.concat(
        [get_all_geohash_from_extent(matrix_bbox[k, :], precision, predicate=predicate, crs=4326)
         for k in range(matrix_bbox.shape[0])]
    )

    # Remove duplicates: might be possible if some of the original input features touch
    shp_geo_all.drop_duplicates(subset='geohash_index', inplace=True)

    return shp_geo_all


def get_subset_geohash_from_gdf(input:  Union[gpd.GeoDataFrame, gpd.geoseries.GeoSeries],
                                precision: int,
                                use_centroid_only: bool = True):
    """Return a single geodataframe containing A SUBSET OF geohashes from a geodataframe by using the centroid from each feature

    If trying to refine a geohash cell, THIS WILL NOT NECESSARILY YIELD ALL THE subgoehahs containing it. It will return either the geohahs associated with the centroid 
    or the polygon extreme points

    Different feature from get_all_geohash_from_extent which returns all geohash inside a bbox
    For this function, we return only the geohash cell over points/simple feature centroids that are in our input geodataframe

    Args:
        input : geo df -> each simple feature gets mapped to a geohash )
        precision: geohash precision
        use_centroid_only: if true, will only encode the centroid of the features, otherwise if geometry is polygon, will encode ALL points
    Returns:
        list[str]: list of geohash indices
    """

    if use_centroid_only:
        # This is lng, lat format
        list_lat_lng = get_matrix_point_coordinates(input)
    else:
        # first 2 columns represent lng and lat
        list_lat_lng = get_matrix_point_polygon(input)[[0, 1]]
        list_lat_lng = list_lat_lng.to_numpy()  # need to convert to n X 2 matrix

    # Watch out encode takes lat, lng
    list_geo_hashes = [geohash.encode(list_lat_lng[k][1], list_lat_lng[k][0], precision)
                       for k in range(list_lat_lng.shape[0])]

    # Get the unique geohash elements only
    shp_geohash = get_geodataframe_from_list_coord(
        [get_geohash_box(hash) for hash in np.unique(list_geo_hashes)],
        crs=4326
    )
    shp_geohash['geohash_index'] = np.unique(list_geo_hashes)

    return shp_geohash


def add_geohash_index(shp_to_add, precision, new_col_name='geohash_index', keep_lat_lng=True):
    """Take a geodataframe and add the geohash index for each feature

    If the lat and lng does not exist, try to add it using each feature's centroid 

    Args:
        shp_to_add : gpd.GeoDataFrame
        precision: int geohash precision e.g. 7
        new_col_name: name of the geohash index column
        keep_lat_lng: bool - keep the lat and lng column added when taking the centroid
    Returns:
        list[str]: list of geohash indices
    """

    # Make a copy, dont want to carry over changes to the original geodf
    shp = shp_to_add.copy()

    # Add lat and lng columns if absent
    if not np.isin(['lat', 'lng'], shp.columns).all():
        logger.debug(
            'Warning in add_geohash_index! No lat lng for geocode encoding: trying to add the centroid')
        shp = add_centroid(shp)

    if (shp[['lat', 'lng']].isna().sum() > 0).any():
        raise ValueError(
            'Fatal error! there are NAs in the lat lng! From add_centroid or initially ')

    if new_col_name in shp.columns:
        logger.warn (
            f'Warning in add_geohash_index! {new_col_name} '\
            f' already exists - dropping and repopulating')
        shp.drop(columns=new_col_name, inplace=True)

    assert np.all(-90 <= shp.lat) and np.all(shp.lat <= 90)
    assert np.all(-180 <= shp.lng) and np.all(shp.lng <= 180)

    geohash_indices = [geohash.encode(lat, lng, precision=precision)
                       for lat, lng in shp[['lat', 'lng']].values]
    shp[new_col_name] = geohash_indices

    if not keep_lat_lng:
        shp.drop(columns=['lat', 'lng'], inplace=True)

    return shp


def recursively_partition_geohash_cells(shp_points,
                                        count_column_name=None,
                                        min_num_points=10,
                                        max_precision=7):
    """From a gpd.GeoDataFrame with point geometry, form a geohash grid with flexible spatial precision.

    Warning! Only works with POINT geometry

    The precision is greater (cells are smaller) in regions with many points

    No spatial join performed, only regular joins by geohash cells after encoding each point's lat lng to a geohash index

    Args:
        shp_points (gpd.GeoDataFrame) :  gpd.GeoDataFrame with Point geometry and a 
        count_column_name (str) (optional) : name of the column that indicates the number of elements per lat lng - if None will count each row as 1 element
        min_num_points (int_): min number of observations in geohash cell required to stop partitioning 
        max_precision (int): max geohash precision
    Returns:
        shp_partionned[gpd.GeoDataFrame], dict_layers[dict]: final gpd geodf + dict where keys indicate precision and values are gpd geodf sufficiently precise at that level
    """

    # QA
    assert isinstance(shp_points, gpd.GeoDataFrame)
    assert min_num_points >= 1, \
        f"Fatal error, cannot use {min_num_points} as " \
        f"a min threshold: use a value >= 1"

    if count_column_name is not None:
        assert count_column_name in shp_points.columns
        print(f'Using the {count_column_name} column for counts')
    else:
        count_column_name='counts'
        shp_points[count_column_name] = np.ones(shp_points.shape[0])
        print(f'No column provided for counts - assuming each row has 1 count')


    assert np.all(shp_points.type == 'Point'), \
        'Code will work with non Point geometry type, '\
        'consider taking the centroid or the points in the exterior of ' \
        'a polygon if this makes sense.'

    # Get the highest precision such that only 1 geohash cell is required
    init_precision = geohash_max_precision(shp_points)

    assert init_precision <= max_precision

    # Get all geohash cells - important to use get_all_geohash_from_extent at the beginning to cover the entire extent
    # And then use get_all_geohash_from_gdf within the for loop to break down each cell that needs refining
    shp_count_by_hash_to_refine = get_all_geohash_from_extent(shp_points,
                                                              init_precision,
                                                              predicate='intersects')

    list_complete = []
    dict_layers = {}

    # Progressively break down cells that have more points than the lower bound threshold
    for p in np.arange(init_precision, max_precision+1):

        # Count observations by hash - always consider all observations
        # Add geohash with precision p to observations
        shp_points_with_hash = add_geohash_index(shp_points, p)

        # Count by geohash index
        df_recent_count_by_hash = count_by_index(
            shp_points_with_hash, 
            group_by_idx="geohash_index", 
            agg_dict={count_column_name  : np.sum}
            )

        # Left join the hash count and fill with 0
        shp_hash_extent = shp_count_by_hash_to_refine.\
            merge(df_recent_count_by_hash[[count_column_name, "geohash_index"]], on="geohash_index", how='left').\
            fillna({count_column_name: 0}).\
            drop_duplicates(subset='geohash_index')

        # Check which cells need refining
        shp_count_by_hash_not_suff_precise = shp_hash_extent.loc[
            shp_hash_extent[count_column_name] > min_num_points, 
            ]
        # Check which cells are sufficiently precise
        shp_count_by_hash_suff_precise = shp_hash_extent.loc[
            shp_hash_extent[count_column_name] <= min_num_points, 
            ].copy()

        # Append final results
        if shp_count_by_hash_suff_precise.shape[0] > 0:
            list_complete.append(shp_count_by_hash_suff_precise)
            dict_layers[p] = shp_count_by_hash_suff_precise

            # Remove the observations
            idx_remove = shp_points_with_hash.\
                merge(
                    shp_count_by_hash_suff_precise, 
                    on='geohash_index')\
                .index

            # These are the points to consider in the next iteration
            shp_points = shp_points.loc[
                ~shp_points.index.isin(idx_remove), 
                ].copy()

        # Check if we reached the max precision and would still require greater precision to meet the threshold condition
        if (p == max_precision) & (shp_count_by_hash_not_suff_precise.shape[0] > 0):
            logger.warn('Warning! Reached the maximum number of iterations (given the max precision) without \
               creating a sufficiently precise geohash grid')
            
            # Still append the cells even if they are not suff precise
            list_complete.append(shp_count_by_hash_not_suff_precise)
            if p in dict_layers.keys():
                dict_layers[p].append(shp_count_by_hash_not_suff_precise)
            else:
                dict_layers[p] = shp_count_by_hash_not_suff_precise

        elif shp_count_by_hash_not_suff_precise.shape[0] > 0:
            # Refine these geohashes at the next precision
            shp_count_by_hash_to_refine = get_all_geohash_from_gdf(
                shp_count_by_hash_not_suff_precise, 
                precision=p+1, 
                predicate='within')
        elif shp_count_by_hash_not_suff_precise.shape[0] == 0:
            # We are done: shp_count_by_hash_not_suff_precise has no rows 
            break

    shp_partionned = pd.concat(list_complete)

    return shp_partionned, dict_layers


# %%
if __name__ == "__main__":

    from geo_py_utils.geo_general.bbox import get_list_coordinates_from_bbox, get_bbox_centroid
    from geo_py_utils.geo_general.geo_utils import get_geodataframe_from_list_coord
    from geo_py_utils.geo_general.map import map_simple
 
    from shapely.geometry import Point, Polygon

    GEOHASH_PRECISION = 6
     
 

    shp_bug = gpd.GeoDataFrame(
            {
            'id' :[0	,1,	2	,3],
            'geohash_index' : ["drgp",	"drgr",	"f0r9",	"f0rb"],
            "lat" : [44.997203	,44.996874	,46.722120	,46.521345],
            "lng": [-74.349024,	-74.084504,	-79.103772	,-78.868221],
            'counts' : [134, 410, 119	,59],
            'geometry': [Point(-74.34902, 44.99720),Point(-74.08450,44.99687),Point(-79.10377,46.72212),Point(-78.86822,46.52135)]
            },
            crs = 4326          
    )
     
    shp_part_bug, _ = recursively_partition_geohash_cells(shp_bug,
                                                    count_column_name='counts',
                                                    min_num_points = 2)