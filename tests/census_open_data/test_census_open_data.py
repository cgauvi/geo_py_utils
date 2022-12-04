from os.path import join, isfile
import geopandas as gpd

 

from geo_py_utils.census_open_data.open_data import download_qc_city_neighborhoods
 

 

def test_qc_city_download():
    download_qc_city_neighborhoods()