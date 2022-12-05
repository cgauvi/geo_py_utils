

from os.path import join
import geopandas as gpd

from geo_py_utils.etl.db_etl import  Url_to_spatialite, Url_to_spatialite   
from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
from geo_py_utils.census_open_data.census import FSA_2016_URL


spatialite_db_path = join(DATA_DIR, "test.db")
table_name = "qc_city_test_tbl"
download_url = QC_CITY_NEIGH_URL

 


def test_spatialite():

    Url_to_spatialite(
        db_name = spatialite_db_path, 
        table_name = table_name,
        download_url = download_url,
        download_destination = DATA_DIR)
