import geopandas as gpd
from os.path import isfile
from pathlib import Path
import pandas as pd
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
from geo_py_utils.utilities_testing.data import SAMPLE_GDF, SAMPLE_GDF_2
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

 
def create_gdf_ext_browser(overwrite, use_1 = True):

    with SnowflakeGeoWriter(
        shp=SAMPLE_GDF if use_1 else SAMPLE_GDF_2,
        sfkl_name=TEST_TBL_NAME,
        sfkl_connector= SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) ,
        sfkl_geo_col_name = SFKL_GEO_COL_NAME,
        overwrite_tbl = overwrite
    ) as sfkl_writer:
        
        sfkl_writer.create_table()
        
  
def create_gdf_ae(overwrite, use_1 = True):

    with SnowflakeGeoWriter(
        shp=SAMPLE_GDF if use_1 else SAMPLE_GDF_2,
        sfkl_name=TEST_TBL_NAME,
        sfkl_connector= SnowflakeAEConnection(PATH_CONFIG_AE) ,
        sfkl_geo_col_name = SFKL_GEO_COL_NAME,
        overwrite_tbl = overwrite
    ) as sfkl_writer:
        
        sfkl_writer.create_table()

@pytest.mark.requires_remote_sfkl_ae
def test_snowflake_types_and_crs_roundtrip_ae():

    create_gdf_ae(overwrite=True)

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
def test_snowflake_types_and_crs_roundtrip_ext_browser():

    create_gdf_ext_browser(overwrite=True)

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
def test_snowflake_overwrite_works():

    create_gdf_ext_browser(overwrite=True)

    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER).get_connection() as conn:
        df_num=pd.read_sql(  f'select count(*) NUM from {TEST_TBL_NAME}', conn )
        n_before = df_num.NUM.values[0]

    create_gdf_ext_browser(overwrite=True)

    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER).get_connection() as conn:
        df_num=pd.read_sql(  f'select count(*) NUM from {TEST_TBL_NAME}', conn )
        n_after = df_num.NUM.values[0]
    
    assert n_after == n_before, 'Fatal error! Overwriting fails'


@pytest.mark.requires_remote_sfkl_interactivity
def test_snowflake_append_works():

    create_gdf_ext_browser(overwrite=True)

    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER).get_connection() as conn:
        df_num=pd.read_sql(  f'select count(*) NUM from {TEST_TBL_NAME}', conn )
        n_before = df_num.NUM.values[0]

    create_gdf_ext_browser(overwrite=False)

    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER).get_connection() as conn:
        df_num=pd.read_sql(  f'select count(*) NUM from {TEST_TBL_NAME}', conn )
        n_after = df_num.NUM.values[0]
     
    assert n_after == SAMPLE_GDF.shape[0] + SAMPLE_GDF_2.shape[0], 'Fatal error! Appending fails'
    
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

if __name__== '__main__':
    test_snowflake_overwrite_works()