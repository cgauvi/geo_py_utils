import dotenv
import os
import pytest
from snowflake.connector.pandas_tools import write_pandas 
import snowflake.connector


from geo_py_utils.etl.snowflake.snowflake_ext_browser_connection import SnowflakeExternalBrowserConnection
from geo_py_utils.etl.snowflake.snowflake_ae_connection import SnowflakeAEConnection
from geo_py_utils.etl.snowflake.db_utils import table_exists
from geo_py_utils.utilities_testing.data import SAMPLE_DF_SAFE
from geo_py_utils.constants import ProjectPaths

TEST_TBL_NAME = 'TEST_TBL'
PATH_CONFIG_DEFAULT = ProjectPaths.CONFIG_PATH / 'snowflake' / 'extbrowser' / ".env"
PATH_CONFIG_AE = ProjectPaths.CONFIG_PATH / 'snowflake' / 'ae' / ".env"

SFKL_CONNECTOR_DEFAULT = SnowflakeExternalBrowserConnection
SFKL_CONNECTOR_AE = SnowflakeAEConnection

def create_df(sfkl_connector = SFKL_CONNECTOR_DEFAULT, path_config = PATH_CONFIG_DEFAULT):
    
    with sfkl_connector(path_config) as connector:
        conn = connector.get_connection()
        write_pandas(
            conn, 
            SAMPLE_DF_SAFE.rename(columns={'id': ' id'}),  # what the actual fuck. `Got snowflake.connector.errors.ProgrammingError` invalid identifier 'ID' and invalid identifier 'id'`  with id or ID
            TEST_TBL_NAME, 
            schema=conn.schema, 
            database=conn.database, 
            overwrite=True
        )

@pytest.mark.requires_remote_sfkl_interactivity
def test_snowflake_tbl_exists(sfkl_connector = SFKL_CONNECTOR_DEFAULT, path_config = PATH_CONFIG_DEFAULT):

    create_df(sfkl_connector, path_config)

    with sfkl_connector(path_config) as connector:
        try:
            conn = connector.get_connection()
            tbl_exists = table_exists(conn, TEST_TBL_NAME)

            assert tbl_exists, 'Table should exist!'
        except Exception as e:
            print(f'Fatal Error - {e}')
        finally:
            try:
                conn.cursor().execute(f"DROP TABLE {TEST_TBL_NAME}")
            except Exception as e:
                print(f'Fatal Error trying to safely drop table {TEST_TBL_NAME} - {e}')

@pytest.mark.requires_remote_sfkl_interactivity
def get_dummy_conn_no_schema(path_config = PATH_CONFIG_DEFAULT):
    dotenv.load_dotenv(path_config)

    conn = snowflake.connector.connect(
                user=os.environ["USER_EMAIL_SFKL"],
                account=os.environ["USER_ACCOUNT_SFKL"],
                authenticator="externalbrowser"
    )

    # Point to the correct DB + schema
    # That way, no need to use statements like `select * from db.schema.table`
    with conn.cursor() as cursor:
            cursor.execute(f"USE ROLE {os.environ['ROLE']};")
            cursor.execute(f"USE WAREHOUSE {os.environ['WAREHOUSE']}")
            cursor.execute(f"USE DATABASE {os.environ['DB']};")
            #deliberately dont set a schema

    return conn

@pytest.mark.requires_remote_sfkl_interactivity
def test_table_exists_with_no_schema(sfkl_connector = SFKL_CONNECTOR_DEFAULT, path_config = PATH_CONFIG_DEFAULT):
    create_df(sfkl_connector, path_config)

    try:

        conn= get_dummy_conn_no_schema(path_config)
        tbl_exists = table_exists(conn, TEST_TBL_NAME)

        assert tbl_exists, 'Table should exist!'

        # Strict check
        with pytest.raises(ValueError):
            table_exists(conn, TEST_TBL_NAME, strict_check_schema=True)


    except Exception as e:
        print(f'Fatal error trying to generate dummy connection without schema - {e}')
    finally :
        try:
            conn.cursor().execute(f"DROP TABLE {TEST_TBL_NAME}")
        except:
            pass

        try:
            conn.close()
        except:
            pass


if __name__ == '__main__':

    test_table_exists_with_no_schema(SFKL_CONNECTOR_DEFAULT, PATH_CONFIG_DEFAULT)