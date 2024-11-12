import geopandas as gpd
import logging
import pandas as pd
from rtree import index

logger = logging.getLogger(__name__)

def intersect_gdf(
        shp_left: gpd.GeoDataFrame,
        shp_left_id: str,
        shp_right: gpd.GeoDataFrame,
        shp_right_id: str,
        new_shp_geometry_name = 'geometry'
        ) -> gpd.GeoDataFrame:
    """
    Manual implementation of `st_intersection` with shapely and geopandas

    Args:
        shp_left (gpd.GeoDataFrame): _description_
        shp_left_id (str): _description_
        shp_right (gpd.GeoDataFrame): _description_
        shp_right_id (str): _description_
        new_shp_geometry_name (str, optional): _description_. Defaults to 'geometry'.

    Returns:
        gpd.GeoDataFrame: new gdf with 1 line per newly created intersected polygon
    """

    # QA
    shp_left_crs = shp_left.crs
    shp_right_crs = shp_right.crs

    if shp_left_crs != shp_right_crs:
        raise ValueError(f'Fatal error! got diff crs: {shp_left_crs} vs {shp_right_crs}')
    
    if shp_left.shape[0] == 0 or shp_right.shape[0]== 0:
        raise ValueError("Fatal error! cannot take intersection, at least 1 of the gdf has 0 rows!")

    crs_init = shp_left_crs

    shp_left_id_new = shp_left_id
    shp_right_id_new = shp_right_id
    if shp_left_id == shp_right_id:
        shp_left_id_new += '_left'
        shp_right_id_new  += '_right'
 
    # Adapted from https://stackoverflow.com/questions/14697442/faster-way-of-polygon-intersection-with-shapely 

    # Populate R-tree index with bounds of grid cells
    idx = index.Index()
    for pos, (_,row) in enumerate(shp_left.iterrows()):
        # assuming cell is a shapely object
        cell = row.geometry.envelope
        idx.insert(pos, cell.bounds)

    # Loop through each Shapely polygon
    all_df_merged = []

    for k,  (_,row) in enumerate(shp_right.iterrows()):

        poly = row.geometry
        intersecting_poly = { pos: shp_left.geometry.iloc[pos] for pos in idx.intersection(poly.bounds)}

        # Now do actual intersection
        shp_intersection = gpd.GeoDataFrame(
            pd.DataFrame(
                    {
                        'index': list(intersecting_poly.keys()),
                        new_shp_geometry_name: [poly.intersection(v) for v in intersecting_poly.values()] 
                    }
            ),
            geometry=new_shp_geometry_name,
            crs=crs_init
        )

        # Save the keys
        shp_intersection[shp_left_id_new] = shp_left[shp_left_id].iloc[ list(intersecting_poly.keys())].values # pd.Series since indexed by list
        shp_intersection [shp_right_id_new] = shp_right[shp_right_id].iloc[k] # single scalar element (e.g. str) since indexed by single value

        # Keep only non empty geometries
        shp_intersection = shp_intersection[~shp_intersection[new_shp_geometry_name].is_empty]

        # append to the list
        all_df_merged.append(shp_intersection)
            
    # concat all
    df_inter = gpd.GeoDataFrame(
        pd.concat(all_df_merged)
    )
    
    if df_inter.shape[0] == 0:
        logger.warning('Warning! st_intersect produced no rows')
    
    # set the crs
    df_inter.set_crs(crs_init, inplace=True)

    return df_inter