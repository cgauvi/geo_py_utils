import pandas as pd
import pytest
import sys

from snowflake.connector.errors import DatabaseError
try:
    from geo_py_utils.etl.snowflake.snowflake_ae_connection import SnowflakeAEConnection
except ImportError:
    pass

from geo_py_utils.constants import ProjectPaths
 
PATH_CONFIG = ProjectPaths.CONFIG_PATH / 'snowflake' / 'ae' / ".env"
 
@pytest.mark.skipif('snowflakewrapper' not in sys.modules,
                    reason="snowflakewrapper condtional: only required for ae platform") 
@pytest.mark.requires_remote_sfkl_ae
def test_conn_sfkl_ae_can_read_from_inf_schema():
        
    query=" select * from M_DATA_SCIENCE.INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'SANDBOX' limit 10 "

    tmp_con = None
    with SnowflakeAEConnection(PATH_CONFIG) as connector:
        conn = connector.get_connection()
        df_results = pd.read_sql(query, conn) 
        tmp_con=conn

    assert df_results.shape[0] > 0 , 'Fatal no results from sso connection!'

    # Connection closed
    with pytest.raises(DatabaseError):
        df_results = pd.read_sql(query, tmp_con) 



@pytest.mark.skipif('snowflakewrapper' not in sys.modules,
                    reason="snowflakewrapper condtional: only required for ae platform") 
@pytest.mark.requires_remote_sfkl_ae
def test_conn_sfkl_ae_db_role_warehouse_properly_set():

    with SnowflakeAEConnection(PATH_CONFIG) as connector:
        conn = connector.get_connection()
        schema = conn.schema
        db = conn.database
        warehouse = conn.warehouse
        role = conn.role 

    assert (
        warehouse is not None and 
        schema is not None and
        db is not None and
        role is not None
    )


if __name__ == '__main__':
    test_conn_sfkl_ae_db_role_warehouse_properly_set()