from warnings import warn
import numpy as np
import geopandas as gpd
import pandas as pd



def add_centroid(shp: gpd.GeoDataFrame, col_names:list =['lng','lat']) ->  gpd.GeoDataFrame:

    """Add the centroid for each of the features in a geodataframe

    Args:
        list_coordinates (geoseries): geoseries - eg. shp['geometry]

    Returns:
        np.array: array with number of vertices per feature/row/polygon
    """

    assert col_names is not None and len(col_names) > 0

    if np.isin(['lat', 'lng'],shp.columns).all() :
        col_names_str = ",".join(col_names)
        warn(f'Warning in add_centroid" {col_names_str} columns already exist')
        return shp

    df_centroids = pd.DataFrame( [ (p.x,p.y) for p in shp.geometry.centroid])
    df_centroids.rename(columns= {0: col_names[0], 1: col_names[1]}, inplace=True)

    # Watch out with the indexing - can create some weird bugs
    shp = pd.concat([shp.reset_index(), df_centroids.reset_index(drop=True)],axis=1) 

    # Remove duplicate columns - might be cause by the concat
    shp = shp.loc[:, ~shp.columns.duplicated() ].copy()

    return shp




def get_centroid_gpd(shp: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """From a geodataframe replace the e.g. Polygon geometry with Point geometry representing the centroid of each simple feature 

 
    Args:
        shp (gpd.GeoDataFrame): geodataframe to use 

    Returns:
        gpd.GeoDataFrame: geodataframe with POINT geometry
    """

    if np.all(shp.type.unique() == 'Point' ):
        warn('Warning! the geodfs geometry is already point: not oing anything')
        return shp

    df_with_lat_lng = add_centroid(shp)

    if 'geometry' in df_with_lat_lng.columns:
        df_with_lat_lng.drop(columns='geometry',inplace=True)

    shp_centroid = gpd.GeoDataFrame( 
        df_with_lat_lng,
        geometry = gpd.points_from_xy(df_with_lat_lng.lng, df_with_lat_lng.lat)
    )

    return shp_centroid