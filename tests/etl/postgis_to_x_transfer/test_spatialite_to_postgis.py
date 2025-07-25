from os.path import exists
import pytest
import logging
import numpy as np

logger = logging.getLogger(__file__)

from geo_py_utils.constants import ProjectPaths
from geo_py_utils.etl.port import is_port_open
from geo_py_utils.etl.postgis.db_utils import pg_drop_table, pg_list_tables, pg_db_exists, pg_create_engine, pg_create_db
from geo_py_utils.etl.postgis.load_spatialite_to_postgis import LoadSqlitePostgis
from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
from geo_py_utils.etl.spatialite.db_utils import list_tables
from geo_py_utils.utilities_testing.data import QcCityTestData
from geo_py_utils.utilities_testing.spatialite_io import upload_qc_neigh_test_db, upload_regular_csv_db


@pytest.fixture
def connection_remote_admin_dev():
    return PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / "aws" / "dev" / ".env")


@pytest.mark.requires_remote_pg_connection_dev
def test_spatialite_to_postgis_remote(connection_remote_admin_dev):

    conn = connection_remote_admin_dev
    dict_str=conn.get_credentials()
    
    # First check
    if not is_port_open(dict_str['host'], dict_str['port']):
        logger.warning(f"Host: {dict_str['host']}:{dict_str['port']} seems closed -> skipping test `test_spatialite_to_postgis_remote`")
        return 

    # Make sure the spatialite db exists
    # Also make sure the geo table exists
    if (not exists(QcCityTestData.SPATIAL_LITE_DB_PATH)) or \
        (not QcCityTestData.SPATIAL_LITE_TBL_QC in list_tables(QcCityTestData.SPATIAL_LITE_DB_PATH)) :
        upload_qc_neigh_test_db(promote_to_multi=False)

    # Make sure the regular (non-geo) table exists
    if not QcCityTestData.SQL_LITE_TBL_CSV_QC in list_tables(QcCityTestData.SPATIAL_LITE_DB_PATH):
        upload_regular_csv_db()

    # Create db connection
    engine = pg_create_engine(
        database=dict_str['database'],
        user=dict_str['user'],
        password=dict_str['password'],
        host=dict_str['host'],
        port=dict_str['port']
    )
    
    # Check if DB exists and create if not
    if not pg_db_exists(engine, dict_str['database']):
        logger.info(f"DB {dict_str['database']} on host {dict_str['host']}:{dict_str['port']}  does not exist: creating it..")
        pg_create_db(database=dict_str['database'],
                    user=dict_str['user'],
                    password=dict_str['password'],
                    host=dict_str['host'],
                    port=dict_str['port'])

    # Drop geo table if exists
    if QcCityTestData.SPATIAL_LITE_TBL_QC in pg_list_tables(engine, dict_str['database']):
        pg_drop_table(engine, dict_str['database'], QcCityTestData.SPATIAL_LITE_TBL_QC)

    # Administrative regions - geo
    sqlite_postgis_loader = LoadSqlitePostgis (
        sqlite_tbl_name=QcCityTestData.SPATIAL_LITE_TBL_QC,
        sqlite_db_name=QcCityTestData.SPATIAL_LITE_DB_PATH,
        postgis_tbl_name=QcCityTestData.SPATIAL_LITE_TBL_QC,
        postgis_db_name=dict_str['database'],
        host=dict_str['host'],
        port=dict_str['port'],
        user=dict_str['user'],
        password=dict_str['password'],
        schema='public',
        promote_to_multi=False,
        overwrite=True
        )

    sqlite_postgis_loader.upload_url_to_database()

    # Teardown
    if QcCityTestData.SPATIAL_LITE_TBL_QC in pg_list_tables(engine, dict_str['database']):
        pg_drop_table(engine, dict_str['database'], QcCityTestData.SQL_LITE_TBL_CSV_QC)

    # Try a regular table 
    sqlite_postgis_loader = LoadSqlitePostgis(
        sqlite_tbl_name=QcCityTestData.SQL_LITE_TBL_CSV_QC,
        sqlite_db_name=QcCityTestData.SPATIAL_LITE_DB_PATH,
        postgis_tbl_name=QcCityTestData.SQL_LITE_TBL_CSV_QC,
        postgis_db_name=dict_str['database'],
        host=dict_str['host'],
        port=dict_str['port'],
        user=dict_str['user'],
        password=dict_str['password'],
        schema='public',
        promote_to_multi=False,
        overwrite=True
        )

    sqlite_postgis_loader.upload_url_to_database()

    # Make sure pg_list_tables works and all tables present
    list_new_tbls = [QcCityTestData.SQL_LITE_TBL_CSV_QC, QcCityTestData.SPATIAL_LITE_TBL_QC]
    list_existing_tables = pg_list_tables(engine, dict_str['database'])
    assert np.all(np.isin(list_new_tbls, list_existing_tables))

     # Teardown
    if QcCityTestData.SQL_LITE_TBL_CSV_QC in pg_list_tables(engine, dict_str['database']):
        pg_drop_table(engine, dict_str['database'], QcCityTestData.SQL_LITE_TBL_CSV_QC)