from os.path import join, exists
import geopandas as gpd
import os
import tempfile
import numpy as np
import pytest
import subprocess
import logging
from sqlalchemy import create_engine


logger = logging.getLogger(__file__)

from geo_py_utils.misc.constants import DATA_DIR
#--
from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
from geo_py_utils.census_open_data.census import FSA_2016_URL
#--
from geo_py_utils.etl.db_etl import Url_to_postgis
from geo_py_utils.etl.port import is_port_open
#--
from geo_py_utils.etl.postgis.load_sfkl_to_postgis import LoadSfklPostgis
from geo_py_utils.etl.postgis.load_spatialite_to_postgis import LoadSqlitePostgis
from geo_py_utils.etl.postgis.db_utils import pg_db_exists, pg_create_db
#--
from geo_py_utils.etl.spatialite.utils_testing import upload_qc_neigh_test_db, QcCityTestData, write_regular_csv_db
from geo_py_utils.etl.spatialite.db_utils import list_tables
 


class LocalPostGIS:
    USER = os.getenv('PG_LOCAL_USER') # defaults to None if not set
    PASSWORD = os.getenv('PG_LOCAL_PASSWORD')
    POSTGIS_DB = 'qc_city_db'
    HOST = "localhost"
    PORT = "5432"
    SCHEMA = "public"

class LocalDockerPostGIS:
    USER = os.getenv('PG_GIC_USER') # defaults to None if not set
    PASSWORD = os.getenv('PG_GIC_PASSWORD')
    POSTGIS_DB = 'test'
    HOST = "localhost"
    PORT = "80"
    SCHEMA = "public"

class RemotePostGIS:
    USER = os.getenv('PG_GIC_USER') # defaults to None if not set
    PASSWORD = os.getenv('PG_GIC_PASSWORD')
    POSTGIS_DB = 'test'
    HOST = os.getenv('PG_GIC_HOST')
    PORT = "5052"
    SCHEMA = "public"



def test_spatialite_local_to_postgis_remote():

    # First check
    if not is_port_open(RemotePostGIS.HOST, RemotePostGIS.PORT):
        logger.warning(f'Host: {RemotePostGIS.HOST}:{RemotePostGIS.PORT} seems closed -> skipping test `test_spatialite_local_to_postgis_remote`')
        return 

    # Make sure the spatialite db exists
    # Also make sure the geo table exists
    if (not exists(QcCityTestData.SPATIAL_LITE_DB_PATH)) or \
        (not QcCityTestData.SPATIAL_LITE_TBL_QC in list_tables(QcCityTestData.SPATIAL_LITE_DB_PATH)) :
        upload_qc_neigh_test_db(promote_to_multi=False)

    # Make sure the regular (non-geo) table exists
    if not  QcCityTestData.SQL_LITE_TBL_CSV_QC in list_tables(QcCityTestData.SPATIAL_LITE_DB_PATH):
        write_regular_csv_db()

    # Create db connection
    engine = create_engine(f'postgresql://{RemotePostGIS.USER}:{RemotePostGIS.PASSWORD}@{RemotePostGIS.HOST}:{RemotePostGIS.PORT}/{RemotePostGIS.POSTGIS_DB}')
    
    # Check if DB exists and create if not
    if not pg_db_exists(engine, RemotePostGIS.POSTGIS_DB):
        logger.info(f"DB {RemotePostGIS.POSTGIS_DB} on host {RemotePostGIS.HOST}:{RemotePostGIS.PORT}  does not exist: creating it..")
        pg_create_db(db_name=RemotePostGIS.POSTGIS_DB,
                    user=RemotePostGIS.USER,
                    password=RemotePostGIS.PASSWORD,
                    host=RemotePostGIS.HOST,
                    port=RemotePostGIS.PORT)

    # Administrative regions
    sqlite_postgis_loader  = LoadSqlitePostgis (
        sqlite_tbl_name=QcCityTestData.SPATIAL_LITE_TBL_QC,
        sqlite_db_name=QcCityTestData.SPATIAL_LITE_DB_PATH,
        postgis_tbl_name=QcCityTestData.SPATIAL_LITE_TBL_QC,
        postgis_db_name=RemotePostGIS.POSTGIS_DB,
        host=RemotePostGIS.HOST,
        port=RemotePostGIS.PORT,
        user=RemotePostGIS.USER,
        password=RemotePostGIS.PASSWORD,
        schema=RemotePostGIS.SCHEMA,
        promote_to_multi=False,
        overwrite=True
        )

    sqlite_postgis_loader.upload_url_to_database()


    # Try a regular table 
    sqlite_postgis_loader = LoadSqlitePostgis(
        sqlite_tbl_name=QcCityTestData.SQL_LITE_TBL_CSV_QC,
        sqlite_db_name=QcCityTestData.SPATIAL_LITE_DB_PATH,
        postgis_tbl_name=QcCityTestData.SQL_LITE_TBL_CSV_QC,
        postgis_db_name=RemotePostGIS.POSTGIS_DB,
        host=RemotePostGIS.HOST,
        port=RemotePostGIS.PORT,
        user=RemotePostGIS.USER,
        password=RemotePostGIS.PASSWORD,
        schema=RemotePostGIS.SCHEMA,
        promote_to_multi=False,
        overwrite=True
        )

    sqlite_postgis_loader.upload_url_to_database()


def test_sfkl_to_postgis_local_docker():
    
    # Quick check for open port
    if not is_port_open(LocalDockerPostGIS.HOST, LocalDockerPostGIS.PORT):
        logger.warning(f'Host: {LocalDockerPostGIS.HOST}:{LocalDockerPostGIS.PORT} seems closed -> skipping test')
        return

    # Create db connection
    engine = create_engine(f'postgresql://{LocalDockerPostGIS.USER}:{LocalDockerPostGIS.PASSWORD}@{LocalDockerPostGIS.HOST}:{LocalDockerPostGIS.PORT}/{LocalDockerPostGIS.POSTGIS_DB}')
    
    # Check if DB exists and create if not
    if not pg_db_exists(engine, LocalDockerPostGIS.POSTGIS_DB):
        logger.info(f"DB {LocalDockerPostGIS.POSTGIS_DB} on host {LocalDockerPostGIS.HOST}:{LocalDockerPostGIS.PORT}  does not exist: creating it..")
        pg_create_db(db_name=LocalDockerPostGIS.POSTGIS_DB,
                    user=LocalDockerPostGIS.USER,
                    password=LocalDockerPostGIS.PASSWORD,
                    host=LocalDockerPostGIS.HOST,
                    port=LocalDockerPostGIS.PORT)

    # Dissemination areas
    sfkl_postgis_loader_agg_tbl = LoadSfklPostgis(
        sfkl_tbl_name="GEOSPATIAL_TEST_BUGS",
        postgis_tbl_name = 'GEO_BUGS',
        host = LocalDockerPostGIS.HOST,
        port = LocalDockerPostGIS.PORT,
        user = LocalDockerPostGIS.USER,
        password = LocalDockerPostGIS.PASSWORD,
        schema = LocalDockerPostGIS.SCHEMA,
        promote_to_multi=False,
        overwrite=True
        )

    sfkl_postgis_loader_agg_tbl.upload_url_to_database()
 



def test_local_shp_to_postgis_local_db_qc():
    """Run test against a local DB - presumably on local linux machine
    """

    psql_exists_results = subprocess.run(["which","psql"], stdout=subprocess.PIPE)
    if len(psql_exists_results.stdout) > 0:


        with Url_to_postgis(
            db_name = LocalPostGIS.POSTGIS_DB, 
            table_name = "qc_city_test_tbl",
            download_url = QC_CITY_NEIGH_URL,
            download_destination = DATA_DIR,
            host = LocalPostGIS.HOST,
            port = LocalPostGIS.PORT,
            user = LocalPostGIS.USER,
            password = LocalPostGIS.PASSWORD,
            schema = LocalPostGIS.SCHEMA,
            ) as uploader:

            if uploader._port_is_open():
                uploader.upload_url_to_database()
            else:
                logger.warning("Skipping test_postgis: port seems closed")
    else:
        logger.warning("Skipping test_postgis: did not find psql on system")


if __name__ == '__main__':

    test_spatialite_local_to_postgis_remote()
    #test_sfkl_to_postgis_local_docker()