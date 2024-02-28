import dotenv
import os
from os.path import abspath, dirname
import pandas as pd
from pathlib import Path
import pytest
from snowflake.connector.pandas_tools import write_pandas 

from geo_py_utils.etl.snowflake.snowflake_connect import connnect_snowflake_ext_browser
from geo_py_utils.etl.snowflake.db_utils import table_exists
from geo_py_utils.misc.utils_test import SAMPLE_DF

HERE = Path(abspath(dirname(__file__)))
TEST_TBL_NAME = 'TEST_TBL'

assert dotenv.load_dotenv(HERE / 'config' / ".env", override=True), 'Config not found!'


@pytest.fixture
def create_df():
    with connnect_snowflake_ext_browser() as conn:
        write_pandas(conn, SAMPLE_DF, TEST_TBL_NAME, schema=conn.schema, database=conn.database, overwrite=True)


@pytest.mark.requires_remote_sfkl_interactivity
def test_snowflake_tbl_exists(create_df):

    create_df

    with connnect_snowflake_ext_browser() as conn:
        tbl_exists = table_exists(conn, TEST_TBL_NAME)

    assert tbl_exists, 'Table should exist!'

    with connnect_snowflake_ext_browser() as conn:
        conn.cursor().execute(f"DROP TABLE {TEST_TBL_NAME}")
