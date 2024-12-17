
from sklearn.cluster import KMeans
import geopandas as gpd
import numpy as np
import pandas as pd

import logging

logger = logging.getLogger(__name__)

def kmeans_centroid_cluster(shp, num_clusters=10) -> gpd.GeoDataFrame:
    """ Cluster spatial features using K means on the feature centroid. 
    
    Can be useful for simple cross validation.

    Not guaranteed to create contiguous clusters. 
    If the original features are all convex, we should expect contiguity but this might also depend on the position of the centroid 

    Args:
        shp (gpd.GeoDataFrame): _description_
        num_clusters (int, optional): number of clusters (up to that number, returns less if not enough simple features). Defaults to 10. 

    Returns:
        _type_: _description_
    """

    assert isinstance(shp,gpd.GeoDataFrame)

    if shp.shape[0] <= num_clusters:
        logger.warning(f"Warning! Cannot return {num_clusters} clusters, there are only {shp.shape[0]} features!")
        shp['spatial_index_k_means'] = np.linspace(start=0, stop=shp.shape[0]-1,  num=shp.shape[0])
        return shp

    matrix_centroids = [ [p.x,p.y] \
        for p in shp.centroid ]

    kmeans= KMeans(n_clusters=num_clusters,random_state=1).\
        fit(matrix_centroids)

    shp['spatial_index_k_means']=pd.Categorical( kmeans.labels_ )

    return shp
