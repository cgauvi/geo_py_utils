import pytest

from geo_py_utils.etl.snowflake.snowflake_ext_browser_connection import SnowflakeExternalBrowserConnection
from geo_py_utils.etl.snowflake.snowflake_ae_connection import SnowflakeAEConnection
from geo_py_utils.etl.snowflake.db_utils import stage_exists
from geo_py_utils.constants import ProjectPaths

PATH_CONFIG_DEFAULT = ProjectPaths.CONFIG_PATH / 'snowflake' / 'extbrowser' / ".env"
PATH_CONFIG_AE = ProjectPaths.CONFIG_PATH / 'snowflake' / 'ae' / ".env"

SFKL_CONNECTOR_DEFAULT = SnowflakeExternalBrowserConnection
SFKL_CONNECTOR_AE = SnowflakeAEConnection


def drop_stages(conn, list_stages=['\"test\"',  '\"TEST\"', 'test',"TEST" ]):


    for l in list_stages:
        v=conn.cursor().execute(f'drop stage if exists {l};')
        print(v.fetchall())


@pytest.mark.requires_remote_sfkl_interactivity
def test_snowflake_stage_exists(sfkl_connector = SFKL_CONNECTOR_DEFAULT, path_config = PATH_CONFIG_DEFAULT):

    with sfkl_connector(path_config) as connector:
        try:
            conn = connector.get_connection()

            conn.cursor().execute("use database M_DATA_SCIENCE;")
            conn.cursor().execute("use SCHEMA SANDBOX;") 
                
            # LC
            conn.cursor().execute('create temporary stage if not exists \"test\"; ')
            assert stage_exists(conn, 'test')

            # UC
            conn.cursor().execute('create temporary stage if not exists  \"TEST\"; ')
            assert stage_exists(conn, 'TEST')
        except :
            pass 
        finally:
            drop_stages(conn)
    

