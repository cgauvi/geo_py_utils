

import subprocess
from os.path import join
import geopandas as gpd
import os

from geo_py_utils.etl.db_etl import  Url_to_spatialite, Url_to_postgis   
from geo_py_utils.etl.gdf_load import spatialite_db_to_gdf
from geo_py_utils.etl.db_utils import list_tables
from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
from geo_py_utils.census_open_data.census import FSA_2016_URL


class Qc_city_data:
    SPATIAL_LITE_DB_PATH = join(DATA_DIR, "test.db")
    SPATIAL_LITE_TBL_QC = "qc_city_test_tbl"
    QC_CITY_NEIGH_URL = QC_CITY_NEIGH_URL


def upload_qc_neigh_db(db_name = Qc_city_data.SPATIAL_LITE_DB_PATH,
                        tbl_name = Qc_city_data.SPATIAL_LITE_TBL_QC,
                        download_url = Qc_city_data.QC_CITY_NEIGH_URL):
    
    with Url_to_spatialite(
        db_name = db_name, 
        table_name = tbl_name,
        download_url = download_url,
        download_destination = DATA_DIR) as uploader:

        uploader.upload_url_to_database()


def test_upload_spatialite_qc():
    upload_qc_neigh_db()



def test_read_gdf_spatialite_qc ():

    if not os.path.exists(Qc_city_data.SPATIAL_LITE_DB_PATH):
        upload_qc_neigh_db()

    df = spatialite_db_to_gdf(db_name = Qc_city_data.SPATIAL_LITE_DB_PATH,
                            tbl_name = Qc_city_data.SPATIAL_LITE_TBL_QC,
                            additional_where_limit_causes = "LIMIT 10"
    ) 

    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] == 10


def test_spatialite_list_tables():

    if not os.path.exists(Qc_city_data.SPATIAL_LITE_DB_PATH):
        upload_qc_neigh_db()

    existing_tables = list_tables(Qc_city_data.SPATIAL_LITE_DB_PATH)

    assert Qc_city_data.SPATIAL_LITE_TBL_QC in existing_tables


def test_spatialite_zip():

    with Url_to_spatialite(
        db_name = join(DATA_DIR, "test_fsa.db"), 
        table_name = 'geo_fsa_tbl',
        download_url = FSA_2016_URL,
        download_destination = DATA_DIR) as uploader:

        uploader.upload_url_to_database()



def test_postgis_qc():

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
            table_name = "qc_city_test_tbl",
            download_url = QC_CITY_NEIGH_URL,
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