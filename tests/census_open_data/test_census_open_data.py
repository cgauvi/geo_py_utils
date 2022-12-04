from os.path import join, isfile
import geopandas as gpd

from ben_py_utils.misc.cache import Cache_wrapper
from ben_py_utils.misc.constants import DEFAULT_CACHE

from geo_py_utils.census_open_data.open_data import download_qc_city_neighborhoods
 

 

def test_qc_city_download():
    download_qc_city_neighborhoods()