import pandas as pd
import pytest
from snowflake.connector.errors import DatabaseError

from geo_py_utils.etl.snowflake.snowflake_ext_browser_connection import SnowflakeExternalBrowserConnection
from geo_py_utils.constants import ProjectPaths
 
PATH_CONFIG = ProjectPaths.CONFIG_PATH / 'snowflake' / 'extbrowser' / ".env"
 
@pytest.mark.requires_remote_sfkl_interactivity
def test_conn_context_manager_improper():
        
    query=" select * from M_DATA_SCIENCE.INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'SANDBOX' limit 10 "

    tmp_con = None
    with SnowflakeExternalBrowserConnection(PATH_CONFIG).get_connection() as conn:
        df_results = pd.read_sql(query, conn) 
        tmp_con=conn

    assert df_results.shape[0] > 0 , 'Fatal no results from sso connection!'

    # Connection closed
    with pytest.raises(DatabaseError):
        df_results = pd.read_sql(query, tmp_con) 


@pytest.mark.requires_remote_sfkl_interactivity
def test_conn_context_manager_proper():
        
    query=" select * from M_DATA_SCIENCE.INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'SANDBOX' limit 10 "

    tmp_con = None
    with SnowflakeExternalBrowserConnection(PATH_CONFIG) as connector:
        conn = connector.get_connection()
        df_results = pd.read_sql(query, conn) 
        tmp_con=conn

    assert df_results.shape[0] > 0 , 'Fatal no results from sso connection!'

    # Connection object still available - only the cursor is closed
    df_results = pd.read_sql(query, tmp_con) 


if __name__ == '__main__':
    test_conn_context_manager_proper()