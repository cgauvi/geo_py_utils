

from asyncio import subprocess
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

    with Url_to_spatialite(
        db_name = spatialite_db_path, 
        table_name = table_name,
        download_url = download_url,
        download_destination = DATA_DIR) as uploader:

        uploader.upload_url_to_database()




def test_postgis():

    import subprocess
    import logging

    logger = logging.getLogger(__name__)

    psql_exists_results = subprocess.run(["which","psql"], stdout=subprocess.PIPE)
    if len(psql_exists_results.stdout) > 0:

        user = os.getenv('PG_LOCAL_USER')
        password = os.getenv('PG_LOCAL_PASSWORD')
        postgis_db_path = 'qc_city_db'

        with Url_to_postgis(
            db_name = postgis_db_path, 
            table_name = table_name,
            download_url = download_url,
            download_destination = DATA_DIR, 
            host = "localhost",
            port = 5432,
            user = user, 
            password = password,
            schema = "public",
            ) as uploader:

            if uploader._port_is_open():
                uploader.upload_url_to_database()
            else:
                logger.warning("Skipping test_postgis: port seems closed")
    else:
        logger.warning("Skipping test_postgis: did not find psql on system")


if __name__ == "__main__":
    test_postgis()