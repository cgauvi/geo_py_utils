

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import scipy.stats as st
import re
import matplotlib.pyplot as plt


def get_2D_kernel_estimate(shp, cell_size=100j, crs=3857, bw_method='scott', range_factor_bbox=0.5):

    """Create a 2D kernel density estimate 

    Returns:
        tuple np.array: _description_
    """

    # Project
    shp['easting'] = shp.to_crs(crs).apply(lambda x: x['geometry'].x, axis=1)
    shp['northing'] = shp.to_crs(crs).apply(lambda x: x['geometry'].y, axis=1)

    # For the 2D kernel estimate: https://stackoverflow.com/questions/30145957/plotting-2d-kernel-density-estimation-with-python
    values = shp[["easting", "northing"]].to_numpy().T  # Needs to have 2 rows

    # Bounding box - make it slightly larger to avoid edge effects
    range_x = (shp.easting.max()-shp.easting.min())
    range_y = (shp.northing.max()-shp.northing.min())
    xmin, xmax = shp.easting.min() - range_factor_bbox * \
        range_x, shp.easting.max() + range_factor_bbox * range_x
    ymin, ymax = shp.northing.min() - range_factor_bbox * \
        range_y, shp.northing.max() + range_factor_bbox * range_y

    # 2D grid
    xx, yy = np.mgrid[xmin:xmax:cell_size, ymin:ymax:cell_size]

    positions = np.vstack([xx.ravel(), yy.ravel()])
    kernel = st.gaussian_kde(values, bw_method=bw_method)
    kern_dens_2d = np.reshape(kernel(positions).T, xx.shape)

    return xx, yy, kern_dens_2d


def convert_2D_kernel_polygon(xx, yy, kern_dens_2d) -> gpd.GeoDataFrame:
    """Create a geodf with polygon geometry representing the 2D KDE

    For contour conversion to Polygon + value extracion.

    Reference:
        https://gis.stackexchange.com/questions/99917/converting-matplotlib-contour-objects-to-shapely-objects

    Args:
        xx (_type_): _description_
        yy (_type_): _description_
        kern_dens_2d (_type_): _description_

    Returns:
        gpd.GeoDataFrame: _description_
    """
    # 
    cs = plt.contour(xx, yy, kern_dens_2d)

    list_polygons = []
    list_vals = []
    for i in range(len(cs.collections)):
        try:
            p = cs.collections[i].get_paths()[0]
            v = p.vertices
            x = v[:, 0]
            y = v[:, 1]
            poly = Polygon([(i[0], i[1]) for i in zip(x, y)])
            list_polygons.append(poly)
            list_vals.append(cs.levels[i])
        except Exception as e:
            print(f'Error in convert_2D_kernel_polygon with {i} - {e}')

    shp_poly = gpd.GeoDataFrame(list_vals,  geometry=list_polygons)

    return shp_poly


def get_polygon_estimate_2D_kernel_from_shp(shp : gpd.geodataframe.GeoDataFrame,
                                            cell_size : int =100j,
                                            col_name_for_counts: int =None,
                                            thresh_density_lb = 0,
                                            *args,
                                            **kwargs):
    """Main entry point that calls get_2D_kernel_estimate to create the 2D functions + convert_2D_kernel_polygon which gets the contours + convert to Polygon


    Args:
        shp (_type_): _description_
        cell_size (_type_, optional): _description_. Defaults to 100j.
        col_name_for_counts (_type_, optional): column name that indicates count/number at that location and should be used to replicate the row lat lng . Defaults to None.
        bw_method (str, optional): Parampassed to scipt.st_gaussian so can be string, float or callable. Defaults to 'scott'.
        thresh_density_lb (float) : minimum value that the density can take -- will only keep simple features such that  density > thresh_density_lb
    Returns:
        _type_: _description_
    """

    assert isinstance(shp, gpd.geodataframe.GeoDataFrame)
    assert shp.shape[0] > 0

    init_crs = shp.crs

    # Multiply the number of rows r times
    # Useful fror instance if a given polygon has e.g. 38 observations
    # Not useful if we only want presence density
    if col_name_for_counts is not None:

        print("Using \'%s\' to multiply each row for 2D KDE estimation"  % col_name_for_counts)

        assert re.match('int', str(
            shp[col_name_for_counts].dtype)), 'Fatal error make sure %s is integer' % col_name_for_counts

        # Replicate rows 
        shp = gpd.GeoDataFrame(
            pd.concat(
                [shp.iloc[np.array(i).repeat(shp[col_name_for_counts].values[i])]
                 for i in range(shp.shape[0])]
            )
        )

    xx, yy, kern_dens_2d = get_2D_kernel_estimate(
        shp, cell_size=cell_size, crs=3857, *args,  **kwargs)

    # Housekeeping - renaming + conversion to initial crs
    shp_poly = convert_2D_kernel_polygon(xx, yy, kern_dens_2d).\
        set_crs(3857).\
        to_crs(init_crs).\
        rename(columns={0: 'density'})

    # Filter out slithers and bugs with density that is excessively small
    shp_poly = shp_poly.loc[shp_poly.density > thresh_density_lb, ]

    return shp_poly
