

import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Polygon
from typing import Union, List, Dict, Any
from typing_extensions import TypeAlias
 
from geo_py_utils.geo_general.geo_utils import get_geodataframe_from_list_coord


dict_gpd: TypeAlias = Dict[Any , gpd.GeoDataFrame]
list_gpd: TypeAlias = List[gpd.GeoDataFrame]




def get_list_coordinates_from_bbox(bbox: Union[List,Dict[str,float]], close_polygon=False) -> List:
    '''Get a list of 4 bounding box scalars [west, south, east, north]
       and return a list of 4 coordinates: 
       [ (west, south),
        (west, north),
        (east, north),
        (east, south)] 
    Bbox should be in the correct order
    '''

    assert len(bbox) == 4, "\
        Fatal error in get_list_coordinates_from_bbox!\n\
        Must input a bounding box with 4 elements\n\
        E.g. with shp.total_bounds\
        "

    if isinstance(bbox,dict) and len(np.intersect1d(list(bbox.keys()), ['east', 'north' , 'south' , 'west'])) == 4:
        west, south, east, north = bbox['west'], bbox['south'], bbox['east'], bbox['north']
    else:
        west, south, east, north = bbox[0], bbox[1], bbox[2], bbox[3]

    list_coord = [
        (west, south),
        (west, north),
        (east, north),
        (east, south)]

    if close_polygon:
        list_coord.extend([(west, south)])

    return list_coord



def get_bbox_group_geometries(collection_geo: Union[dict_gpd, list_gpd],
                              by_feature=False) -> tuple:
    """Get the bounding box from a collection of geodataframes 
 

    Args:
        collection_geo ( group of geodf ):  
        by_feature ( bool ) :  If true, get the bounding box per feature, otherwise get the total bounds

    Returns:
        tuple: longitude/easting, latitude/northing
    """

    concat_gpd = pd.concat(collection_geo)

    if by_feature:
        bounds = [g.bounds for g in concat_gpd.geometry]
    else:
        bounds = concat_gpd.total_bounds

    return bounds


def get_bbox_centroid(input: Union[gpd.GeoDataFrame, list]) -> tuple:
    """Get the overall centroid from a geodataframe using the bounding box (or you can send in the raw bounding box too)

    Args:
        shp (_type_): _description_

    Returns:
        tuple: longitude/easting, latitude/northing
    """

    if isinstance(input, gpd.GeoDataFrame):
        bbox_bounds = get_bbox_gpd(input).total_bounds
    else:
        bbox_bounds = input

    lng_central, lat_central = (
        bbox_bounds[2]+bbox_bounds[0])/2, (bbox_bounds[3]+bbox_bounds[1])/2

    return (lng_central, lat_central)


def get_bbox_gpd(shp: gpd.GeoDataFrame,
                 crs=None) -> gpd.GeoDataFrame:
    """Get a bounding box as a geodataframe from an initial geodataframe

    Args:
        shp (_type_): _description_

    Returns:
        _type_: _description_
    """

    assert isinstance(shp, gpd.GeoDataFrame)

    crs_init = shp.crs

    if crs is None:
        # Project to web marcator
        shp_proj = shp.to_crs(3857)
    else:
        shp_proj = shp

    # Get bbox coordinates & Convert to polyon
    bbox = Polygon(get_list_coordinates_from_bbox(
        shp_proj.total_bounds, close_polygon=True))

    # To geodataframe
    shp_bbox = gpd.GeoDataFrame({'id': [1], "geometry": bbox},
                                geometry="geometry",
                                crs=shp_proj.crs).\
        to_crs(crs_init)

    return shp_bbox


def get_bbox_extent(extents: Union[List,Dict[str,float]], crs) -> gpd.GeoDataFrame:
    """Small convenience function that takes in a list or a dict with 4 elements west, south, east, north and returns a polygon

    Args:
        extents (_type_): list or a dict with 4 elements west, south, east
        crs (int)
    Returns:
        _type_: _description_
    """
    list_coordinates = get_list_coordinates_from_bbox(extents) #4 1D points -> 4 2D points 
    shp = get_geodataframe_from_list_coord ( [list_coordinates], crs=crs )

    return shp
