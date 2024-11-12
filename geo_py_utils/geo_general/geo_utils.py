""" Geospatial functions for general use """


import numpy as np
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape, Polygon, LineString, Point, GeometryCollection
from typing import  Union

LIST_VALID_PREDICATES = ['within', 'strictly within', 'intersects', 'contains', 'strictly contains', 'touches']

def compare_two_gdf_predicate(
        gdf_left: gpd.GeoDataFrame,
        gdf_right: gpd.GeoDataFrame,
        predicate: str,
        bool_fun = all
        ) -> np.array :
    
    """
    Compare 2 gdf feature by feature (cartesian join) and returns a series indicating if any of the left features are related to the right 

    Example:
    ```
    
    gdf1_within_2 = gdf1[compare_two_gdf_predicate(gdf1, gdf2, 'within')]

    ```

    Raises:
        ValueError: If invalid predicate

    Returns:
         np.array (bool): Same size as gdf_left.shape[0] so can be used as indexing e.g. `gdf_left[ compare_two_gdf_predicate (gdf_left, gdf_right, 'intersects') ]`
    """


    if not predicate in LIST_VALID_PREDICATES:
        raise ValueError(f'Fatal error! predicate should be within {LIST_VALID_PREDICATES}') 
    
    if predicate == 'contains':
        # "Every point of b is a point of (the interior or boundary of) a"
        df_join = pd.DataFrame(
            { 
                k: gdf_left.covers(g) for
                k,g in enumerate(gdf_right.geometry)
            } 
        )
    elif predicate == 'strictly contains':
        # geometry b lies in a, and the interiors intersect
        df_join = pd.DataFrame(
            { 
                k: gdf_left.covers(g) & ~gdf_left.boundary.intersects(g.boundary)  for
                k,g in enumerate(gdf_right.geometry)
            } 
        )

    elif predicate == 'within':
        # Every point of a is a point of (the interior or boundary of) b
        df_join = pd.DataFrame(
            { 
                k: gdf_left.covered_by(g) for
                k,g in enumerate(gdf_right.geometry)
            } 
        )
    elif predicate == 'strictly within':
        #a is within b: a lies in the interior of b.	
        df_join = pd.DataFrame(
            { 
                k: gdf_left.covered_by(g) & ~gdf_left.boundary.intersects(g.boundary)  for
                k,g in enumerate(gdf_right.geometry)
            } 
        )
    elif predicate == 'intersects':
        # a and b have at least one point in common.	
        df_join = pd.DataFrame(
            { 
                k: gdf_left.intersects(g)  for
                k,g in enumerate(gdf_right.geometry)
            } 
        )
    elif predicate == 'touches':
        # a and b have at least one point in common, but their interiors do not intersect.   
        df_join = pd.DataFrame(
            { 
                k: gdf_left.touches(g)  for
                k,g in enumerate(gdf_right.geometry)
            } 
        )
    else:
        raise ValueError(f'Invalid predicate {predicate}. Choose from {LIST_VALID_PREDICATES}')
    
    return df_join.apply(bool_fun, axis=1).values

def explode_multiparts(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    
    if not isinstance(gdf, gpd.GeoDataFrame):
        raise TypeError('Fatal error in explode_multiparts: input not GeoDataFrame')
    
    # no GeometryCollection
    gdf = (
        gdf
        .reset_index()
        .explode(index_parts =False)
    ) #otherwise, indexing gets messed up

    return gdf

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

    if not isinstance(shp, gpd.GeoDataFrame):
        raise TypeError('Fatal error in get_matrix_point_polygon: input not GeoDataFrame')
    if not np.isin(shp.type.unique(), ['Multipolygon', 'Polygon']).all():
        raise RuntimeError('Fatal error in get_matrix_point_polygon: only works with polygon or multipolygon: call `explode_multiparts` first and filter first or `geo_py_utils.geo_general.valid_geom:make_extra_valid_gpd` ' )

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

def get_num_point_polygon(geo: Polygon) -> int:
    if geo.geom_type.startswith("Multi"):
        n = 0
        for part in geo.geoms:
            n += len(part.exterior.coords)
            for ring in part.interiors:
                n += len(ring.coords)
    else:
        n = len(geo.exterior.coords)
        for ring in geo.interiors:
            n += len(ring.coords)

    return n

def get_num_point_point(geo: Point) -> int:
    if geo.geom_type.startswith("Multi"):
        n=0
        for part in geo.geoms:
            n+=1
    else:
        n = 1

    return n

def get_num_point_linestring(geo: LineString) -> int:
    if geo.geom_type.startswith("Multi"):
        n = 0
        for part in geo.geoms:
            n += len(part.coords)
    else:
        n = len(geo.coords)
    return n

def get_num_points_in_geoseries(geoseries: gpd.GeoSeries) -> np.array:
    """Count the number of coordinates in each feature in a geoseries

   Args:
        list_coordinates (geoseries): geoseries - eg. shp['geometry]

    Returns:
        np.array: array with number of vertices per feature/row/polygon
    """
    n_vertices = []
    for geo in geoseries:

        if "Line" in geo.geom_type:
            n=get_num_point_linestring(geo)
        elif "Point" in geo.geom_type:
            n=get_num_point_point(geo)
        elif "Poly" in geo.geom_type:
             n=get_num_point_polygon(geo)
        elif 'GeometryCollection':
            n=np.sum(get_num_points_in_geoseries(geoseries.explode()))
        else:
            raise RuntimeError(f'Error! invalid gem type: {geo.geom_type}')

        n_vertices.append(n)

    return np.array(n_vertices)

 
 