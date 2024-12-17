
import geopandas as gpd
from requests.exceptions import RequestException

from geo_py_utils.etl.download_zip import download_zip_shp
 
def test_download_stats_can():

    try:
        shp = download_zip_shp ('https://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lfsa000b16a_e.zip')

        assert isinstance(shp, gpd.GeoDataFrame)
    except RequestException as e:
        print(f'Network error in test - {e} - skipping')

