import pytest
from snowflake.connector.pandas_tools import write_pandas 

from geo_py_utils.etl.snowflake.snowflake_ext_browser_connection import SnowflakeExternalBrowserConnection
from geo_py_utils.etl.snowflake.db_utils import table_exists
from geo_py_utils.misc.utils_test import SAMPLE_DF_SAFE
from geo_py_utils.constants import ProjectPaths

TEST_TBL_NAME = 'TEST_TBL'
PATH_CONFIG = ProjectPaths.CONFIG_PATH / 'snowflake' / 'extbrowser' / ".env"

def create_df():
    with SnowflakeExternalBrowserConnection(PATH_CONFIG) as connector:
        conn = connector.get_connection()
        write_pandas(
            conn, 
            SAMPLE_DF_SAFE, 
            TEST_TBL_NAME, 
            schema=conn.schema, 
            database=conn.database, 
            overwrite=True
        )

@pytest.mark.requires_remote_sfkl_interactivity
def test_snowflake_tbl_exists():

    create_df()

    with SnowflakeExternalBrowserConnection(PATH_CONFIG) as connector:
        conn = connector.get_connection()
        tbl_exists = table_exists(conn, TEST_TBL_NAME)

        assert tbl_exists, 'Table should exist!'

        conn.cursor().execute(f"DROP TABLE {TEST_TBL_NAME}")


if __name__ == '__main__':

    test_snowflake_tbl_exists()