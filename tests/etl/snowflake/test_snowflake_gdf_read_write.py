import geopandas as gpd
from os.path import isfile
from pathlib import Path
import pytest

from geo_py_utils.etl.snowflake.gdf_load import sfkl_to_gpd
from geo_py_utils.etl.snowflake.snowflake_ext_browser_connection import SnowflakeExternalBrowserConnection

try:
    from geo_py_utils.etl.snowflake.snowflake_ae_connection import SnowflakeAEConnection
except ImportError:
    pass

import sys

from geo_py_utils.etl.snowflake.db_utils import sfkl_get_table_srid
from geo_py_utils.etl.snowflake.gdf_write import SnowflakeGeoWriter
from geo_py_utils.misc.utils_test import SAMPLE_GDF
from geo_py_utils.constants import ProjectPaths
 
PATH_CONFIG_EXT_BROWSER = ProjectPaths.CONFIG_PATH / 'snowflake' / 'extbrowser' / ".env"
PATH_CONFIG_AE = ProjectPaths.CONFIG_PATH / 'snowflake' / 'ae' / ".env"
TEST_TBL_NAME = 'TEST_SHP_WRITE_SFKL'
SFKL_GEO_COL_NAME= 'GEO_TEST'

@pytest.mark.skipif('snowflakewrapper' not in sys.modules,
                    reason="snowflakewrapper condtional: only required for ae platform") 
def teardown_ae():
    with SnowflakeAEConnection(PATH_CONFIG_AE) as connector:
        connector.get_connection().cursor().execute(f"DROP TABLE if exists {TEST_TBL_NAME}")
 
def teardown_ext_browser():
    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) as connector:
        connector.get_connection().cursor().execute(f"DROP TABLE if exists {TEST_TBL_NAME}")

@pytest.fixture
def create_gdf_ext_browser():

    with SnowflakeGeoWriter(
        shp=SAMPLE_GDF,
        sfkl_name=TEST_TBL_NAME,
        sfkl_connector= SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) ,
        sfkl_geo_col_name = SFKL_GEO_COL_NAME
    ) as sfkl_writer:
        
        sfkl_writer.create_table()
        
  
@pytest.fixture
def create_gdf_ae():

    with SnowflakeGeoWriter(
        shp=SAMPLE_GDF,
        sfkl_name=TEST_TBL_NAME,
        sfkl_connector= SnowflakeAEConnection(PATH_CONFIG_AE) ,
        sfkl_geo_col_name = SFKL_GEO_COL_NAME
    ) as sfkl_writer:
        
        sfkl_writer.create_table()

@pytest.mark.requires_remote_sfkl_ae
def test_snowflake_types_and_crs_roundtrip_ae(create_gdf_ae):

    create_gdf_ae

    with SnowflakeAEConnection(PATH_CONFIG_AE) as connector: 
        shp_round_trip = sfkl_to_gpd(
            query = f'SELECT * from {TEST_TBL_NAME} limit 10', 
            conn = connector.get_connection(), 
            tbl_name = TEST_TBL_NAME,
            geometry_name = SFKL_GEO_COL_NAME
            )
        crs_sfkl = sfkl_get_table_srid(connector.get_connection(), TEST_TBL_NAME, SFKL_GEO_COL_NAME)

    assert isinstance(shp_round_trip, gpd.GeoDataFrame), 'Fatal error! Roundtrip data not read in as geodf!'
    assert shp_round_trip.crs.to_epsg() == crs_sfkl, 'Fatal error! Roundtrip crs from snowflake and geodf do not match'
    
    teardown_ae()

@pytest.mark.requires_remote_sfkl_interactivity
def test_snowflake_types_and_crs_roundtrip_ext_browser(create_gdf_ext_browser):

    create_gdf_ext_browser

    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) as connector: 
        shp_round_trip = sfkl_to_gpd(
            query = f'SELECT * from {TEST_TBL_NAME} limit 10', 
            conn = connector.get_connection(), 
            tbl_name = TEST_TBL_NAME,
            geometry_name = SFKL_GEO_COL_NAME
            )
        crs_sfkl = sfkl_get_table_srid(connector.get_connection(), TEST_TBL_NAME, SFKL_GEO_COL_NAME)

    assert isinstance(shp_round_trip, gpd.GeoDataFrame), 'Fatal error! Roundtrip data not read in as geodf!'
    assert shp_round_trip.crs.to_epsg() == crs_sfkl, 'Fatal error! Roundtrip crs from snowflake and geodf do not match'
    
    teardown_ext_browser()

@pytest.mark.requires_remote_sfkl_interactivity
def test_temp_files_deleted_ext_browser():

    with SnowflakeGeoWriter(
        shp=SAMPLE_GDF,
        sfkl_name=TEST_TBL_NAME,
        sfkl_connector=SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) 
    ) as sfkl_writer:
        
        sfkl_writer.create_table()
        local_tmp_file_path_no_ext = Path(sfkl_writer.local_tmp_file_path).stem # remove extension

    # Context manager should clean up
    ext=['csv','csvt','prj']
    files_with_ext = [f'{local_tmp_file_path_no_ext}.{e}' for e in ext]
    for f in files_with_ext:
        assert not isfile(f)

    teardown_ext_browser()

 