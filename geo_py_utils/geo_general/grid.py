

import numpy as np
from shapely.geometry import Polygon
import geopandas as gpd
from math import ceil



def get_grid_over_shp(shp: gpd.GeoDataFrame,
                      height_y_m: float = None,
                      width_x_m: float = None,
                      cell_dim: tuple = None,
                      crs=None) -> gpd.GeoDataFrame:
    """Generate a rectangular grid over a geodataframe extent 

    Based on https://gis.stackexchange.com/questions/269243/creating-polygon-grid-using-geopandas

    Args:
        shp (gpd.GeoDataFrame): _description_
        height_y_m: height/lenth of each cell in meters
        width_x_m: width of each cell in meters
        crs: crs used - to ensure we are working in meters and not degrees, Default = 3857
    Returns:
        gpd.GeoDataFrame: _description_
    """

    # ^ is xor
    assert (cell_dim is not None) ^ (height_y_m is not None and width_x_m is not None), \
        'Fatal error! use either the deired number of cells of set the cell width and height manually '

    # Project
    crs_init = shp.crs
    crs_used = 3857 if crs is None else crs_init
    shp_proj = shp.to_crs(crs_used)

    # Get the extent/bounding box
    xmin, ymin, xmax, ymax = shp_proj.total_bounds

    # Get the cell dimensions based on parameters
    if (height_y_m is not None and width_x_m is not None):

        if height_y_m > (xmax - xmin):
            raise ValueError(
                'Warning! cannot use such a large heigth, the total extent height is %f' % (xmax - xmin))

        if width_x_m > (ymax - ymin):
            raise ValueError(
                'Warning! cannot use such a large width, the total extent width is %f' % (ymax - ymin))

    else:

        assert len(cell_dim) == 2

        width_x_m = ceil((xmax - xmin) / cell_dim[0])
        height_y_m = ceil((ymax - ymin) / cell_dim[1])

    # Create the list of points (bottom left corner for each cell)
    cols = list(np.arange(xmin, xmax + width_x_m, width_x_m))
    rows = list(np.arange(ymin, ymax + height_y_m, height_y_m))

    # Create the polygons
    polygons = []
    for x in cols[:-1]:
        for y in rows[:-1]:
            polygons.append(
                Polygon([(x, y), (x+width_x_m, y), (x+width_x_m, y+height_y_m), (x, y+height_y_m)]))

    # Convert to geodataframe + reproject
    shp_grid = gpd.GeoDataFrame({'geometry': polygons}, crs=crs_used).\
        to_crs(crs_init)

    return shp_grid


if __name__ == '__main__':

    import contextily as ctx

    url = 'https://www.donneesquebec.ca/recherche/dataset/5b1ae6f2-6719-46df-bd2f-e57a7034c917/resource/436c85aa-88d9-4e57-9095-b72b776a71a0/download/vdq-quartier.geojson'

    shp_neigh = gpd.read_file(url)

    #shp_grid = get_grid_ver_shp(shp_neigh)

    ax = shp_neigh.to_crs(3857).plot(column='NOM', alpha=0.5)

    ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)
