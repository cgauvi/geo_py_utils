# From https://stackoverflow.com/questions/3104781/inverse-distance-weighted-idw-interpolation-with-python

from __future__ import division
import numpy as np
from scipy.spatial import cKDTree as KDTree
# http://docs.scipy.org/doc/scipy/reference/spatial.html
import geopandas as gpd
import pandas as pd
from insights.utilities.util_geo import get_matrix_point_coordinates
from typing import Union

# ...............................................................................


class Invdisttree:
    """ inverse-distance-weighted interpolation using KDTree:
invdisttree = Invdisttree( X, z )  -- data points, values
interpol = invdisttree( q, nnear=3, eps=0, p=1, weights=None, stat=0 )
    interpolates z from the 3 points nearest each query point q;
    For example, interpol[ a query point q ]
    finds the 3 data points nearest q, at distances d1 d2 d3
    and returns the IDW average of the values z1 z2 z3
        (z1/d1 + z2/d2 + z3/d3)
        / (1/d1 + 1/d2 + 1/d3)
        = .55 z1 + .27 z2 + .18 z3  for distances 1 2 3

    q may be one point, or a batch of points.
    eps: approximate nearest, dist <= (1 + eps) * true nearest
    p: use 1 / distance**p
    weights: optional multipliers for 1 / distance**p, of the same shape as q
    stat: accumulate wsum, wn for average weights

How many nearest neighbors should one take ?
a) start with 8 11 14 .. 28 in 2d 3d 4d .. 10d; see Wendel's formula
b) make 3 runs with nnear= e.g. 6 8 10, and look at the results --
    |interpol 6 - interpol 8| etc., or |f - interpol*| if you have f(q).
    I find that runtimes don't increase much at all with nnear -- ymmv.

p=1, p=2 ?
    p=2 weights nearer points more, farther points less.
    In 2d, the circles around query points have areas ~ distance**2,
    so p=2 is inverse-area weighting. For example,
        (z1/area1 + z2/area2 + z3/area3)
        / (1/area1 + 1/area2 + 1/area3)
        = .74 z1 + .18 z2 + .08 z3  for distances 1 2 3
    Similarly, in 3d, p=3 is inverse-volume weighting.

Scaling:
    if different X coordinates measure different things, Euclidean distance
    can be way off.  For example, if X0 is in the range 0 to 1
    but X1 0 to 1000, the X1 distances will swamp X0;
    rescale the data, i.e. make X0.std() ~= X1.std() .

A nice property of IDW is that it's scale-free around query points:
if I have values z1 z2 z3 from 3 points at distances d1 d2 d3,
the IDW average
    (z1/d1 + z2/d2 + z3/d3)
    / (1/d1 + 1/d2 + 1/d3)
is the same for distances 1 2 3, or 10 20 30 -- only the ratios matter.
In contrast, the commonly-used Gaussian kernel exp( - (distance/h)**2 )
is exceedingly sensitive to distance and to h.

    """
# anykernel( dj / av dj ) is also scale-free
# error analysis, |f(x) - idw(x)| ? todo: regular grid, nnear ndim+1, 2*ndim

    def __init__(self, X, z, leafsize=10):
        assert len(X) == len(z), "len(X) %d != len(z) %d" % (len(X), len(z))
        self.tree = KDTree(X, leafsize=leafsize)  # build the tree
        self.z = z
        self.wn = 0
        self.wsum = None

    def __call__(self, q, nnear=6, eps=0, p=1, weights=None):
        # nnear nearest neighbours of each query point --
        q = np.asarray(q)
        qdim = q.ndim
        if qdim == 1:
            q = np.array([q])
        if self.wsum is None:
            self.wsum = np.zeros(nnear)

        self.distances, self.ix = self.tree.query(q, k=nnear, eps=eps)
        interpol = np.zeros((len(self.distances),) + np.shape(self.z[0]))
        jinterpol = 0
        for dist, ix in zip(self.distances, self.ix):
            if nnear == 1:
                wz = self.z[ix]
            elif dist[0] < 1e-10:
                wz = self.z[ix[0]]
            else:  # weight z s by 1/dist --
                w = 1 / dist**p
                if weights is not None:
                    w *= weights[ix]  # >= 0
                w /= np.sum(w)
                wz = np.dot(w, self.z[ix])
            interpol[jinterpol] = wz
            jinterpol += 1
        return interpol if qdim > 1 else interpol[0]


def interpolate_points_idw(shp_known: gpd.GeoDataFrame,
                           col_variable_to_interpolate: str,
                           shp_to_interpolate: gpd.GeoDataFrame,
                           nnear:int =4 , 
                           eps :float =0, 
                           p :int =2, 
                           crs = 3857,
                           tree_kwargs = {'leafsize':10}) -> gpd.GeoDataFrame:

    """Interpolate geostatistical (point based) values using deterministic inverse distance weighted methd.

    Args:
        shp_known (gpd.GeoDataFrame): _description_
        col_variable_to_interpolate (str): _description_
        shp_to_interpolate (gpd.GeoDataFrame): geo df with POINT geometry
        nnear (int, optional): _description_. Defaults to 4.
        eps (float, optional): _description_. Defaults to 0.
        p (int, optional): _description_. Defaults to 2.
        crs (int, optional): _description_. Defaults to 3857.
        tree_kwargs (dict, optional): _description_. Defaults to {'leafsize':10}.

    Returns:
        gpd.GeoDataFrame: geo df with interpolated values
    """

    assert np.isin(col_variable_to_interpolate, shp_known.columns).all()

    # Project to web mercator or other projection 
    crs_init = shp_known.crs

    shp_known = shp_known.to_crs(crs)
    shp_to_interpolate = shp_to_interpolate.to_crs(crs)

    # Get the positions of points for which we know the value to interpolate
    known_values = shp_known[col_variable_to_interpolate]
    known_coordinates = get_matrix_point_coordinates(shp_known)

    # Get the positions of new points for which we want to interpolate the value to interpolate
    # Easier to use geodataframe since crs is attached
    assert isinstance(shp_to_interpolate, gpd.GeoDataFrame)
    df_pos = pd.DataFrame( get_matrix_point_coordinates(shp_to_interpolate) ,columns=['x','y'])

    # Build the kd tree for nearest neighbor computation
    invdisttree = Invdisttree(known_coordinates,
                              known_values,
                              **tree_kwargs)

    # IDW interpolation
    interpol = invdisttree(df_pos[['x', 'y']], nnear=nnear, eps=eps, p=p)

    shp_interpolate = gpd.GeoDataFrame(df_pos,
                                       geometry=gpd.points_from_xy(x=df_pos.x, y=df_pos.y),
                                       crs=crs)

    shp_interpolate[col_variable_to_interpolate] = interpol

    # Reproject 
    shp_interpolate = shp_interpolate.to_crs(crs_init)


    return shp_interpolate