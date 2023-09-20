
import geopandas as gpd

import os; import sys; from os import environ; sys.path.append(os.path.join(environ['PATH_TO_GIC'],'gic','geo_py_utils'))

from geo_py_utils.etl.download_zip import download_zip_shp
 
def test_download_stats_can():

    shp = download_zip_shp ('https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lfsa000b16a_e.zip')

    assert isinstance(shp, gpd.GeoDataFrame)

if __name__ == '__main__':
    test_download_stats_can()