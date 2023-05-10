from os.path import exists
import os
import numpy as np
import pytest
import subprocess
import logging


logger = logging.getLogger(__file__)

from geo_py_utils.misc.constants import DATA_DIR
#--
from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
#--
from geo_py_utils.etl.db_etl import Url_to_postgis
from geo_py_utils.etl.port import is_port_open
#--
from geo_py_utils.etl.postgis.load_sfkl_to_postgis import LoadSfklPostgis
from geo_py_utils.etl.postgis.load_spatialite_to_postgis import LoadSqlitePostgis
from geo_py_utils.etl.postgis.db_utils import (
    pg_db_exists, 
    pg_create_db, 
    pg_list_tables, 
    pg_drop_table, 
    pg_create_engine,
    pg_create_ogr2ogr_str,
    pg_create_connection_str
)

#--
from geo_py_utils.etl.spatialite.utils_testing import upload_qc_neigh_test_db, QcCityTestData, write_regular_csv_db
from geo_py_utils.etl.spatialite.db_utils import list_tables
 


class LocalPostGIS:
    USER = os.getenv('PG_LOCAL_USER')
    PASSWORD = os.getenv('PG_LOCAL_PASSWORD')
    POSTGIS_DB = 'test'
    HOST = "localhost"
    PORT = "5432"
    SCHEMA = "public"
    TBL_NAME = "qc_city_test_tbl"

class LocalDockerPostGIS:
    USER = os.getenv('PG_GIC_USER')  # use same password as RemotePostGIS
    PASSWORD = os.getenv('PG_GIC_PASSWORD')
    POSTGIS_DB = 'test'
    HOST = "localhost"
    PORT = "80"
    SCHEMA = "public"

class RemotePostGIS:
    USER = os.getenv('PG_GIC_USER')
    PASSWORD = os.getenv('PG_GIC_PASSWORD')
    POSTGIS_DB = 'test'
    HOST = os.getenv('PG_GIC_HOST')
    PORT = os.getenv('PG_GIC_PORT')
    SCHEMA = "public"

class SfklTbl:
    SFKL_TBL_NAME = 'GEOSPATIAL_TEST_BUGS'
    POSTGIS_TBL_NAME = 'GEOSPATIAL_TEST_BUGS'
    GEOMETRY_COLUMN_NAME = 'GEOMETRY'


def test_conn_str():

    dict_str={
        "db_name": RemotePostGIS.POSTGIS_DB,
        "user": RemotePostGIS.USER,
        "password": 'dummy_pass',
        "host": RemotePostGIS.HOST,
        "port": RemotePostGIS.PORT
        }

    assert pg_create_ogr2ogr_str(**dict_str) == f' "PG:host={RemotePostGIS.HOST} dbname={RemotePostGIS.POSTGIS_DB} user={RemotePostGIS.USER} password=dummy_pass port={RemotePostGIS.PORT}" '
    assert pg_create_connection_str(**dict_str) == f'postgresql://{RemotePostGIS.USER}:dummy_pass@{RemotePostGIS.HOST}:{str(RemotePostGIS.PORT)}/{RemotePostGIS.POSTGIS_DB}'


        

@pytest.mark.requires_remote_pg_connection_sfkl_interactivity
def test_sflk_to_postgis_remote():

    # First check
    if not is_port_open(RemotePostGIS.HOST, RemotePostGIS.PORT):
        logger.warning(f'Host: {RemotePostGIS.HOST}:{RemotePostGIS.PORT} seems closed -> skipping test `test_spatialite_local_to_postgis_remote`')
        return

    # Create db connection
    engine = pg_create_engine(
        database=RemotePostGIS.POSTGIS_DB,
        user=RemotePostGIS.USER,
        password=RemotePostGIS.PASSWORD,
        host=RemotePostGIS.HOST,
        port=RemotePostGIS.PORT
    )
    
    # Drop table if exists
    if SfklTbl.SFKL_TBL_NAME in pg_list_tables(engine, RemotePostGIS.POSTGIS_DB):
        pg_drop_table(engine, RemotePostGIS.POSTGIS_DB, SfklTbl.SFKL_TBL_NAME)

    # Bugs dataset
    sfkl_postgis_loader_agg_tbl = LoadSfklPostgis(
        sfkl_tbl_name=SfklTbl.SFKL_TBL_NAME,
        sfkl_geometry_name=SfklTbl.GEOMETRY_COLUMN_NAME,
        postgis_tbl_name=SfklTbl.SFKL_TBL_NAME,
        postgis_db_name=RemotePostGIS.POSTGIS_DB,
        host=RemotePostGIS.HOST,
        port=RemotePostGIS.PORT,
        user=RemotePostGIS.USER,
        password=RemotePostGIS.PASSWORD,
        schema=RemotePostGIS.SCHEMA,
        promote_to_multi=False,
        overwrite=True
        )

    sfkl_postgis_loader_agg_tbl.upload_url_to_database()


@pytest.mark.requires_remote_pg_connection
def test_list_tables_postgis_remote():

    # First check
    if not is_port_open(RemotePostGIS.HOST, RemotePostGIS.PORT):
        logger.warning(f'Host: {RemotePostGIS.HOST}:{RemotePostGIS.PORT} seems closed -> skipping test `test_spatialite_local_to_postgis_remote`')
        return

    DB_NAME_DOES_NOT_EXIST = "RANDOMTBLDOESNOTEXIST"

    # Create db connection
    engine = pg_create_engine(
        database=DB_NAME_DOES_NOT_EXIST,
        user=RemotePostGIS.USER,
        password=RemotePostGIS.PASSWORD,
        host=RemotePostGIS.HOST,
        port=RemotePostGIS.PORT
    )
    
    assert pg_list_tables(engine, DB_NAME_DOES_NOT_EXIST) == []

    

@pytest.mark.requires_remote_pg_connection
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
    if not QcCityTestData.SQL_LITE_TBL_CSV_QC in list_tables(QcCityTestData.SPATIAL_LITE_DB_PATH):
        write_regular_csv_db()

    # Create db connection
    engine = pg_create_engine(
        database=RemotePostGIS.POSTGIS_DB,
        user=RemotePostGIS.USER,
        password=RemotePostGIS.PASSWORD,
        host=RemotePostGIS.HOST,
        port=RemotePostGIS.PORT
    )
    
    # Check if DB exists and create if not
    if not pg_db_exists(engine, RemotePostGIS.POSTGIS_DB):
        logger.info(f"DB {RemotePostGIS.POSTGIS_DB} on host {RemotePostGIS.HOST}:{RemotePostGIS.PORT}  does not exist: creating it..")
        pg_create_db(database=RemotePostGIS.POSTGIS_DB,
                    user=RemotePostGIS.USER,
                    password=RemotePostGIS.PASSWORD,
                    host=RemotePostGIS.HOST,
                    port=RemotePostGIS.PORT)

    # Drop table if exists
    if QcCityTestData.SPATIAL_LITE_TBL_QC in pg_list_tables(engine, RemotePostGIS.POSTGIS_DB):
        pg_drop_table(engine, RemotePostGIS.POSTGIS_DB, QcCityTestData.SPATIAL_LITE_TBL_QC)

    # Administrative regions
    sqlite_postgis_loader = LoadSqlitePostgis (
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

    # Drop table if exists
    if QcCityTestData.SQL_LITE_TBL_CSV_QC in pg_list_tables(engine, RemotePostGIS.POSTGIS_DB):
        pg_drop_table(engine, RemotePostGIS.POSTGIS_DB, QcCityTestData.SQL_LITE_TBL_CSV_QC)

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

    # Make sure pg_list_tables works and all tables present
    list_new_tbls = [QcCityTestData.SQL_LITE_TBL_CSV_QC, QcCityTestData.SPATIAL_LITE_TBL_QC]
    list_existing_tables = pg_list_tables(engine, RemotePostGIS.POSTGIS_DB)
    assert np.all(np.isin(list_new_tbls, list_existing_tables))


@pytest.mark.requires_local_docker_pg_connection
def test_sfkl_to_postgis_local_docker():
    
    # Quick check for open port
    if not is_port_open(LocalDockerPostGIS.HOST, LocalDockerPostGIS.PORT):
        logger.warning(f'Host: {LocalDockerPostGIS.HOST}:{LocalDockerPostGIS.PORT} seems closed -> skipping test')
        return

    # Create db connection
    engine = pg_create_engine(database=LocalDockerPostGIS.POSTGIS_DB,
                    user=LocalDockerPostGIS.USER,
                    password=LocalDockerPostGIS.PASSWORD,
                    host=LocalDockerPostGIS.HOST,
                    port=LocalDockerPostGIS.PORT)
    
    # Check if DB exists and create if not
    if not pg_db_exists(engine, LocalDockerPostGIS.POSTGIS_DB):
        logger.info(f"DB {LocalDockerPostGIS.POSTGIS_DB} on host {LocalDockerPostGIS.HOST}:{LocalDockerPostGIS.PORT}  does not exist: creating it..")
        pg_create_db(database=LocalDockerPostGIS.POSTGIS_DB,
                    user=LocalDockerPostGIS.USER,
                    password=LocalDockerPostGIS.PASSWORD,
                    host=LocalDockerPostGIS.HOST,
                    port=LocalDockerPostGIS.PORT)

    # Drop table if exists
    if SfklTbl.SFKL_TBL_NAME in pg_list_tables(engine, LocalDockerPostGIS.POSTGIS_DB):
        pg_drop_table(engine, LocalDockerPostGIS.POSTGIS_DB, SfklTbl.SFKL_TBL_NAME)

    # Bugs dataset
    sfkl_postgis_loader_agg_tbl = LoadSfklPostgis(
        sfkl_tbl_name=SfklTbl.SFKL_TBL_NAME,
        sfkl_geometry_name=SfklTbl.GEOMETRY_COLUMN_NAME,
        postgis_tbl_name=SfklTbl.SFKL_TBL_NAME,
        postgis_db_name=LocalDockerPostGIS.POSTGIS_DB,
        host=LocalDockerPostGIS.HOST,
        port=LocalDockerPostGIS.PORT,
        user=LocalDockerPostGIS.USER,
        password=LocalDockerPostGIS.PASSWORD,
        schema=LocalDockerPostGIS.SCHEMA,
        promote_to_multi=False,
        overwrite=True
        )

    sfkl_postgis_loader_agg_tbl.upload_url_to_database()


@pytest.mark.requires_local_pg_connection
def test_local_shp_to_postgis_local_db_qc():
    """Run test against a local DB - presumably on local linux machine
    """

    psql_exists_results = subprocess.run(["which","psql"], stdout=subprocess.PIPE)
    if len(psql_exists_results.stdout) > 0:


        with Url_to_postgis(
            db_name = LocalPostGIS.POSTGIS_DB,
            table_name = LocalPostGIS.TBL_NAME,
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