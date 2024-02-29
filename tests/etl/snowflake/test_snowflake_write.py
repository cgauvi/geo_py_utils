import dotenv
import geopandas as gpd
from os.path import abspath, dirname, isfile
import pandas as pd
from pathlib import Path
import pytest

from geo_py_utils.etl.snowflake.snowflake_read import sfkl_to_gpd, sfkl_get_table_srid
from geo_py_utils.etl.snowflake.snowflake_connect import connnect_snowflake_ext_browser
from geo_py_utils.etl.snowflake.db_utils import table_exists
from geo_py_utils.etl.snowflake.snowflake_write import SnowflakeGeoWriter
from geo_py_utils.misc.utils_test import SAMPLE_GDF

HERE = Path(abspath(dirname(__file__)))
TEST_TBL_NAME = 'TEST_SHP_WRITE_SFKL'
PATH_CONFIG = HERE / 'config' / ".env"
SFKL_GEO_COL_NAME= 'GEO_TEST'

assert dotenv.load_dotenv(PATH_CONFIG, override=True), 'Config not found!'

def teardown():
    with connnect_snowflake_ext_browser() as conn:
        conn.cursor().execute(f"DROP TABLE {TEST_TBL_NAME}")

@pytest.fixture
def create_gdf():

    with SnowflakeGeoWriter(
        shp=SAMPLE_GDF,
        sfkl_name=TEST_TBL_NAME,
        path_env_file=PATH_CONFIG,
        sfkl_geo_col_name = SFKL_GEO_COL_NAME
    ) as sfkl_writer:
        
        sfkl_writer.create_table()
        
 
@pytest.mark.requires_remote_sfkl_interactivity
def test_snowflake_tbl_exists(create_gdf):

    create_gdf

    with connnect_snowflake_ext_browser() as conn:
        tbl_exists = table_exists(conn, TEST_TBL_NAME)

    assert tbl_exists, 'Table should exist!'

    teardown()

@pytest.mark.requires_remote_sfkl_interactivity
def test_snowflake_types_and_crs_roundtrip(create_gdf):

    create_gdf

    with connnect_snowflake_ext_browser() as conn:  
        shp_round_trip = sfkl_to_gpd(
            query = f'SELECT * from {TEST_TBL_NAME} limit 10', 
            conn = conn, 
            tbl_name = TEST_TBL_NAME,
            geometry_name = SFKL_GEO_COL_NAME
            )
        crs_sfkl = sfkl_get_table_srid(conn, TEST_TBL_NAME, SFKL_GEO_COL_NAME)

    assert isinstance(shp_round_trip, gpd.GeoDataFrame), 'Fatal error! Roundtrip data not read in as geodf!'
    assert shp_round_trip.crs.to_epsg() == crs_sfkl, 'Fatal error! Roundtrip crs from snowflake and geodf do not match'
    
    teardown()

@pytest.mark.requires_remote_sfkl_interactivity
def test_temp_files_deleted():

    with SnowflakeGeoWriter(
        shp=SAMPLE_GDF,
        sfkl_name=TEST_TBL_NAME,
        path_env_file=PATH_CONFIG
    ) as sfkl_writer:
        
        sfkl_writer.create_table()

        local_tmp_file_path_no_ext = Path(sfkl_writer.local_tmp_file_path).stem # remove extension
        ext=['csv','csvt','prj']
        files_with_ext = [f'{local_tmp_file_path_no_ext}.{e}' for e in ext]
        for f in files_with_ext:
            assert not isfile(f)

    teardown()

if __name__ == '__main__':
    test_temp_files_deleted()