
import pytest

from geo_py_utils.etl.snowflake.db_utils import sfkl_show_column, sfkl_get_specific_col_types
from geo_py_utils.etl.snowflake.snowflake_ext_browser_connection import SnowflakeExternalBrowserConnection
from geo_py_utils.etl.snowflake.gdf_write import SnowflakeGeoWriter
from geo_py_utils.utilities_testing.data import SAMPLE_GDF, SFKL_EXPECTED_MAPPING, GEOG_COL_NAME
from geo_py_utils.constants import ProjectPaths
 
 
TEST_TBL_NAME = 'TEST_SHP_WRITE_SFKL'
PATH_CONFIG = ProjectPaths.CONFIG_PATH / 'snowflake' / 'extbrowser' / ".env"
 
COLS_EXISTING_SUBSET = [k.upper() for (i,k) in enumerate(SFKL_EXPECTED_MAPPING.keys()) if i <= 3 ]
COLS_NOT_ALL_EXIST = COLS_EXISTING_SUBSET + ['somerandomnewcolumnthatdoesnotexist']

def create_gdf():

    with SnowflakeGeoWriter(
        shp=SAMPLE_GDF,
        sfkl_name=TEST_TBL_NAME,
        sfkl_connector=SnowflakeExternalBrowserConnection(PATH_CONFIG),
        sfkl_geo_col_name=GEOG_COL_NAME
    ) as sfkl_writer:
        
        sfkl_writer.create_table()


def teardown():
    with SnowflakeExternalBrowserConnection(PATH_CONFIG) as connector:
        connector.get_connection().cursor().execute(f"DROP TABLE if exists {TEST_TBL_NAME}")


@pytest.mark.requires_remote_sfkl_interactivity
def test_col_types_geo():

    create_gdf()

    with SnowflakeExternalBrowserConnection(PATH_CONFIG) as connector:
        conn = connector.get_connection()
        df_results = sfkl_show_column(conn, TEST_TBL_NAME)
        df_results_subset = sfkl_get_specific_col_types(conn, TEST_TBL_NAME, COLS_EXISTING_SUBSET)

        with pytest.raises(Exception):
            sfkl_get_specific_col_types(conn, TEST_TBL_NAME, COLS_NOT_ALL_EXIST)

    assert df_results.shape[0] == SAMPLE_GDF.shape[1], 'Error! not all columns present'


    for col in COLS_EXISTING_SUBSET:
        real_col_type_1 = df_results[df_results.column_name == col].json_col.values[0]
        assert  real_col_type_1 == SFKL_EXPECTED_MAPPING[col.lower()], \
            f'Col: {col}. Expected from `sfkl_show_column`: { SFKL_EXPECTED_MAPPING[col.lower()]} vs real: {real_col_type_1}'
    
        real_col_type_2 = df_results_subset[df_results.column_name == col].data_type.values[0]
        assert  real_col_type_2 == SFKL_EXPECTED_MAPPING[col.lower()], \
            f'Col: {col}. Expected from `sfkl_get_specific_col_types`: { SFKL_EXPECTED_MAPPING[col.lower()]} vs real: {real_col_type_2}'
 

    teardown()

        
if __name__ == '__main__':
    test_col_types_geo()