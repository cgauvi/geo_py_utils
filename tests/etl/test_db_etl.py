

from os.path import join
import geopandas as gpd
import os

from geo_py_utils.etl.db_etl import  Url_to_spatialite, Url_to_postgis   
from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
from geo_py_utils.census_open_data.census import FSA_2016_URL



table_name = "qc_city_test_tbl"
download_url = QC_CITY_NEIGH_URL

 


def test_spatialite():

    spatialite_db_path = join(DATA_DIR, "test.db")

    uploader = Url_to_spatialite(
        db_name = spatialite_db_path, 
        table_name = table_name,
        download_url = download_url,
        download_destination = DATA_DIR)

    uploader.upload_url_to_database()




def test_postgis():

    user = os.environ['PG_LOCAL_USER']
    password = os.environ['PG_LOCAL_PASSWORD']
    postgis_db_path = 'qc_city_db'

    uploader = Url_to_postgis(
        db_name = postgis_db_path, 
        table_name = table_name,
        download_url = download_url,
        download_destination = DATA_DIR, 
        host = "localhost",
        port = 5432,
        user = user, 
        password = password,
        schema = "public",
        )

    uploader.upload_url_to_database()

if __name__ == "__main__":
    test_postgis()