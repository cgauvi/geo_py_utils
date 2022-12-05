
import geopandas as gpd

from geo_py_utils.etl.download_zip import download_zip_shp
 
def test_download_stats_can():

    shp = download_zip_shp ('https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lfsa000b16a_e.zip')

    assert isinstance(shp, gpd.GeoDataFrame)