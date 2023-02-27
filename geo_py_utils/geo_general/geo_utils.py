""" Geospatial functions for general use """


import numpy as np
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
from typing import  Union

def flip_coord(list_points):
    new_poly = []
    for k in range(len(list_points)):
        new_poly.append([ [ p[1],p[0] ]  for p in list_points[k] ])

    return new_poly 


def get_matrix_point_polygon(shp: gpd.GeoDataFrame) -> pd.DataFrame:
    """Get the polyon points from a Multipolygon or Polygon and return the result as a 2D dataframe with the polygon id (corresponding to the shp index)

    Args:
        shp (gpd.GeoDataFrame): _description_

    Returns:
        np.array: _description_
    """

    assert isinstance(shp, gpd.GeoDataFrame)
    assert np.isin(shp.type.unique(), ['Multipolygon', 'Polygon']).all()

    dict_coords = {
        k: row['geometry'].exterior.coords.xy for k, row in shp.iterrows()}

    matrix_coords = {k: np.concatenate(
        [np.array(dict_coords[k][0]).reshape(-1, 1),
         np.array(dict_coords[k][1]).reshape(-1, 1)], axis=1) for k in dict_coords.keys()
    }

    df_coordinates_by_poly = pd.concat([pd.DataFrame(matrix_coords[k]).assign(poly_id=k)
                                        for k in matrix_coords.keys()])

    return df_coordinates_by_poly


def get_matrix_point_coordinates(shp: Union[gpd.GeoDataFrame, gpd.geoseries.GeoSeries]) -> np.array:
    """Convert geometry column to a 2 column matrix by taking centroids

    Args:
        shp (gpd.GeoDataFrame): _description_

    Returns:
        np.array: _description_
    """
    assert isinstance(shp, gpd.GeoDataFrame) or isinstance(shp, gpd.geoseries.GeoSeries), \
        'Fatal error in get_matrix_point_coordinates! needs to be a geodf or geoseries'

    if isinstance(shp, gpd.GeoDataFrame):
        geo_array = np.array(
            [np.array((shp['geometry'][k].centroid.xy[0][0],
                       shp['geometry'][k].centroid.xy[1][0]))
             for k in range(shp.shape[0])]
        )
    else:
        geo_array = np.array(
            [np.array((shp[k].centroid.xy[0][0],
                       shp[k].centroid.xy[1][0]))
             for k in range(shp.shape[0])]
        )

    return geo_array


def get_geodataframe_from_list_coord(list_coordinates: list, crs):
    """Given a list of coordinates/points representing a SINGLE polygon
     in the following format: [ [ [lng1,lat1], [lng2,lat2] ] ] or 
                              [ ((lng1,lat1), (lng2,lat2)) ]
    Convert to a geodataframe

    Args:
        list_coordinates (list): list of points 
        crs (_type_): reference system used 

    Returns:
        gpd.GeoDataFrame: geodataframe with 
    """
    json_poly = [
        {"type": "Polygon",
         "coordinates": [coord]
         }
        for coord in list_coordinates]

    # Convert to geodataframe
    shp = gpd.GeoDataFrame(pd.DataFrame({'index': range(len(list_coordinates))}),
                           geometry=[shape(j)
                                     for j in json_poly],
                           crs=4326).\
        to_crs(crs)

    return shp

def get_num_points_in_geoseries(geoseries: gpd.GeoSeries):
    """Count the number of coordinates in each feature in a geoseries

   Args:
        list_coordinates (geoseries): geoseries - eg. shp['geometry]

    Returns:
        np.array: array with number of vertices per feature/row/polygon
    """
    n_vertices = []
    for geo in geoseries:

        if geo.type.startswith("Multi"):
            n = 0
            # iterate over all parts of multigeometry
            for part in geo.geoms:
                n += len(part.exterior.coords)
        else:
            n = len(geo.exterior.coords)

        n_vertices.append(n)

    return np.array(n_vertices)

 
 