from scipy.stats.qmc import Sobol
import geopandas as gpd
import pandas as pd
import numpy as np
from math import ceil

def generate_random_points_over_extent(shp: gpd.GeoDataFrame,
                                       num_points_to_gen: int ,
                                       method: str ='grid',
                                       **kwargs) -> gpd.GeoDataFrame:
    """Generate random points from a geodataframe extent 

    Dispatch based on method choose from ['sobol','grid']

    Args:
        method (str): method to use for sampling
        Depending on method, see
         generate_random_points_over_extent_sobolo or 
         generate_random_points_over_extent_grid method definition

    Returns:
        gpd.GeoDataFrame: sampled POINT geodataframe
    """
    assert np.isin(method, ['grid', 'sobol']).any()

    if method == "grid":
        shp = generate_random_points_over_extent_grid(shp, num_points_to_gen, **kwargs)
    else:
        shp = generate_random_points_over_extent_sobol(shp, num_points_to_gen, **kwargs)

    return shp


def generate_random_points_over_extent_grid(shp: gpd.GeoDataFrame, num_points_to_gen, crs=None) -> gpd.GeoDataFrame:
    """Generate random points from a geodataframe extent in a grid-lie fashion

    Samples point according to the shape of the boundinx box (proportional to each side's length)

    Args:
        shp (gpd.GeoDataFrame): _description_
        num_points_to_gen (_type_): _description_
        crs (_type_, optional): _description_. Defaults to None.

    Returns:
        gpd.GeoDataFrame: _description_
    """
    crs_init = shp.crs
    crs_used = 3857 if crs is None else crs_init
    shp_proj = shp.to_crs(crs_used)

    # Project and get bounding box
    west, south, east, north = shp_proj.total_bounds

    max_side = max((north - south), (east - west))
    min_side = min((north - south), (east - west))

    if (north - south) > (east - west):
        ratio_north_south = max_side / (max_side + min_side)
        ratio_east_west = 1 - ratio_north_south
    else:
        ratio_north_south = 1 - (max_side / (max_side + min_side))
        ratio_east_west = 1 - ratio_north_south

    # Generate random points over grid
    # We will multiply these and we can get more than num_points_to_gen because of ceil
    # Truncate at the end
    num_north_south = ceil(num_points_to_gen * ratio_north_south)
    num_east_west = ceil(num_points_to_gen * ratio_east_west)

    northing_rand_pos = south + \
        np.random.uniform(size=(num_north_south)) * (north-south)
    easting_rand_pos = west + \
        np.random.uniform(size=(num_east_west)) * (east-west)

    # Full join to recover grid
    df_pos_north = pd.DataFrame(
        {"y": northing_rand_pos, "dummy": np.ones_like(northing_rand_pos)})
    df_pos_east = pd.DataFrame(
        {"x": easting_rand_pos, "dummy": np.ones_like(easting_rand_pos)})

    df_pos = pd.merge(df_pos_east, df_pos_north, on='dummy', how='outer')

    # Convert to geodaframe and return
    shp_pos = gpd.GeoDataFrame(
        df_pos,
        geometry=gpd.points_from_xy(
            x=df_pos.x, y=df_pos.y),
        crs=crs_used).\
        to_crs(crs_init)

    return(shp_pos.iloc[:num_points_to_gen])


def generate_random_points_over_extent_sobol(shp: gpd.GeoDataFrame, num_points_to_gen, crs=None) -> gpd.GeoDataFrame:
    """Generate random points from a geodataframe extent in a grid-lie fashion

    Samples point according to the shape of the bounding box (proportional to each side's length)

    Args:
        shp (gpd.GeoDataFrame): _description_
        num_points_to_gen (_type_): _description_
        crs (_type_, optional): _description_. Defaults to None.

    Returns:
        gpd.GeoDataFrame: _description_
    """
    crs_init = shp.crs
    crs_used = 3857 if crs is None else crs_init
    shp_proj = shp.to_crs(crs_used)

    # Project and get bounding box
    west, south, east, north = shp_proj.total_bounds

    width = (east - west)
    height = (north - south)

    # Generate random points according to sobol sequence
    # Sampled points are in [0,1)^2
    sobol_sampler = Sobol(d=2, scramble=True)
    sampled_0_1_points = sobol_sampler.random(n=num_points_to_gen)

    # Get the final coordinates with affine transform
    # Starting from south east corner of bbox, using the width and height multiplied by an offset
    # Multiply column by column
    mat_offset = np.multiply(sampled_0_1_points,  np.array( [width, height]) ) 

    # origin is the south west corner - replicated num_points_to_gen - once for each point
    mat_origin = np.ndarray.repeat( np.array([west,south]).reshape(-1,1) , num_points_to_gen ,axis=1).T

    mat_pos = mat_offset + mat_origin  

    df_pos = pd.DataFrame(mat_pos, columns=['x', 'y'])

    # Convert to geodaframe and return
    shp_pos = gpd.GeoDataFrame(
        df_pos,
        geometry=gpd.points_from_xy(
            x=df_pos.x, y=df_pos.y),
        crs=crs_used).\
        to_crs(crs_init)

    return(shp_pos.iloc[:num_points_to_gen])

