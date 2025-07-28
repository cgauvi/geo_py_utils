import logging
from os.path import abspath, dirname
import pytest
from pathlib import Path

logger = logging.getLogger(__file__)

#--
from geo_py_utils.etl.port import is_port_open
#--
from geo_py_utils.etl.postgis.db_utils import (
    pg_list_tables, 
    pg_create_engine,
    pg_create_ogr2ogr_str,
    pg_create_connection_str,
    pg_sql_to_df,
    pg_get_table_rows
)
from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
#--
from geo_py_utils.constants import ProjectPaths

HERE = Path(abspath(dirname(__file__)))
TBL_NAME = "qc_city_test_tbl"


@pytest.fixture
def connection_localhost():
    return PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / "localhost" / ".env")


@pytest.fixture
def connection_remote_admin_dev():
    return PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / "aws" / "dev" / ".env")


@pytest.fixture
def connection_remote_ro_dev():
    return PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / "aws" / "dev" / "ro.env")



@pytest.mark.requires_remote_pg_connection_dev
def test_conn_str(connection_remote_admin_dev):

    conn = connection_remote_admin_dev
    dict_str=conn.get_credentials()

    assert pg_create_ogr2ogr_str(**dict_str)  == f"PG:'host={dict_str['host']} dbname={dict_str['database']} user={dict_str['user']} password={dict_str['password']} port={dict_str['port']}'"
    assert pg_create_connection_str(**dict_str) == f'postgresql://{dict_str["user"]}:{dict_str["password"]}@{dict_str["host"]}:{str(dict_str["port"])}/{dict_str["database"]}'


     
@pytest.mark.requires_local_pg_connection 
def test_list_tables_postgis_local(connection_localhost):

    conn = connection_localhost
    dict_str=conn.get_credentials()

    # Create db connection
    engine = pg_create_engine(
        database=dict_str['database'],
        user=dict_str['user'],
        password=dict_str['password'],
        host=dict_str['host'],
        port=dict_str['port']
    )
    
    assert len(pg_list_tables(engine, dict_str['database'])) > 0




@pytest.mark.requires_remote_pg_connection_dev
def test_list_tables_postgis_remote_dev(connection_remote_admin_dev):

    conn = connection_remote_admin_dev
    dict_str=conn.get_credentials()

    # First check
    if not is_port_open(dict_str['host'], dict_str['port']):
        logger.warning(f"Host: {dict_str['host']}:{dict_str['port']} seems closed -> skipping test `test_spatialite_local_to_postgis_remote`")
        return

    DB_NAME_DOES_NOT_EXIST = "RANDOMTBLDOESNOTEXIST"

    # Create db connection
    engine = pg_create_engine(
        database=DB_NAME_DOES_NOT_EXIST,
        user=dict_str['user'],
        password=dict_str['password'],
        host=dict_str['host'],
        port=dict_str['port']
    )
    
    assert pg_list_tables(engine, DB_NAME_DOES_NOT_EXIST) == []

    


@pytest.mark.requires_remote_pg_connection_dev_ro
def test_list_tables_postgis_remote_dev_ro(connection_remote_ro_dev):

    DB_NAME_DOES_NOT_EXIST = "RANDOMTBLDOESNOTEXIST"

    # Create db connection
    engine = pg_create_engine(**connection_remote_ro_dev.get_credentials())

    assert pg_list_tables(engine, DB_NAME_DOES_NOT_EXIST) == []
 
 
@pytest.mark.requires_remote_pg_connection_dev
def test_row_count(connection_remote_admin_dev):

    # Create db connection
    engine = pg_create_engine(**connection_remote_admin_dev.get_credentials())

    df_results = pg_sql_to_df( 
        engine, 
        query= 'select count(*) num from spatial_ref_sys',
        is_spatial = False
    )
    

    num_rows_check= pg_get_table_rows(engine, "spatial_ref_sys")

    assert df_results.num.values[0] == num_rows_check


@pytest.mark.requires_remote_pg_connection_dev
def test_kwargs_sql_df(connection_remote_admin_dev):

    CHUNK_SIZE=10

    # Create db connection
    engine = pg_create_engine(**connection_remote_admin_dev.get_credentials())

    df_iter = pg_sql_to_df( 
        engine, 
        query= 'select *  from spatial_ref_sys',
        is_spatial = False,
        chunksize=CHUNK_SIZE
    )
    
    assert CHUNK_SIZE == next(df_iter).shape[0]
  