import logging
import numpy as np
import pandas as pd
from pathlib import Path
import pytest
from snowflake.connector.errors import ProgrammingError
import sys
 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

try:
    from geo_py_utils.etl.snowflake.snowflake_ae_connection import SnowflakeAEConnection
except ImportError:
    pass


from geo_py_utils.etl.snowflake.snowflake_ext_browser_connection import SnowflakeExternalBrowserConnection
from geo_py_utils.etl.snowflake.csv_write import SnowflakeCsvWriter, DEFAULT_FORMAT_TYPE_OPTIONS
from geo_py_utils.etl.snowflake.db_utils import sfkl_show_column, stage_exists, table_exists
from geo_py_utils.utilities_testing.data import SAMPLE_DF
from geo_py_utils.utilities_testing.tmp_file import TempFile
from geo_py_utils.constants import ProjectPaths
 
PATH_CONFIG_EXT_BROWSER = ProjectPaths.CONFIG_PATH / 'snowflake' / 'extbrowser' / ".env"
PATH_CONFIG_AE = ProjectPaths.CONFIG_PATH / 'snowflake' / 'ae' / ".env"
TEST_TBL_NAME = 'TEST_CSV_WRITE_SFKL'


@pytest.mark.skipif('snowflakewrapper' not in sys.modules,
                    reason="snowflakewrapper condtional: only required for ae platform") 
def teardown_ae():
    with SnowflakeAEConnection(PATH_CONFIG_AE) as connector:
        connector.get_connection().cursor().execute(f"DROP TABLE if exists {TEST_TBL_NAME}")
        connector.get_connection().cursor().execute(f"DROP STAGE if exists {TEST_TBL_NAME}")
 
def teardown_ext_browser():
    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) as connector:
        connector.get_connection().cursor().execute(f"DROP TABLE if exists {TEST_TBL_NAME}")
        connector.get_connection().cursor().execute(f"DROP STAGE if exists {TEST_TBL_NAME}")

@pytest.mark.requires_remote_sfkl_interactivity
def test_csv_upload_success_existing_stage():

    sql_stage_find = "  select STAGE_NAME  from information_schema.stages  "
    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) as connector:
        df_stage_find = pd.read_sql(sql_stage_find, connector.get_connection())
    
    if df_stage_find.shape[0] > 0:
        with TempFile() as temp_csv:
            # Create the temp csv
            SAMPLE_DF.to_csv(temp_csv.name,index=False)

            # upload to snowflake
            with SnowflakeCsvWriter(
                csv_path=temp_csv.name,
                sfkl_name=TEST_TBL_NAME,
                sfkl_stage=df_stage_find['STAGE_NAME'].values[0],
                sfkl_connector= SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) ,
                overwrite_tbl = True
            ) as sfkl_writer:
                
                sfkl_writer.create_table()

   
    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) as connector:
         # stage not deleted
        assert stage_exists(connector.get_connection(), df_stage_find['STAGE_NAME'].values[0])
        # table exists 
        assert table_exists(connector.get_connection(), TEST_TBL_NAME)

def create_df_ext_browser(overwrite):

    with TempFile() as temp_csv:
        # Create the temp csv
        SAMPLE_DF.to_csv(temp_csv.name,index=False)

        # upload to snowflake
        with SnowflakeCsvWriter(
            csv_path=temp_csv.name,
            sfkl_name=TEST_TBL_NAME,
            sfkl_connector= SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) ,
            overwrite_tbl = overwrite
        ) as sfkl_writer:
            
            sfkl_writer.create_table()

 
def create_df_ae(overwrite):

    with TempFile() as temp_csv:
        # Create the temp csv
        SAMPLE_DF.to_csv(temp_csv.name,index=False)

        with SnowflakeCsvWriter(
            shp=temp_csv.name,
            sfkl_name=TEST_TBL_NAME,
            sfkl_connector= SnowflakeAEConnection(PATH_CONFIG_AE) ,
            overwrite_tbl = overwrite
        ) as sfkl_writer:
            
            sfkl_writer.create_table()

@pytest.mark.requires_remote_sfkl_ae
def test_snowflake_roundtrip_ae():

    create_df_ae(overwrite=True)

    with SnowflakeAEConnection(PATH_CONFIG_AE) as connector: 
        df_round_trip = pd.read_sql(f'SELECT * from {TEST_TBL_NAME}', con = connector.get_connection())

    assert df_round_trip.shape[0] == SAMPLE_DF.shape[0], 'Fatal error! Roundtrip data not read in as df with right number of rows!'
 
    teardown_ae()
 

@pytest.mark.requires_remote_sfkl_interactivity
def test_hwa_ext_browser_fail():

    # Removing the important `FIELD_OPTIONALLY_ENCLOSED_BY` field from the csv format options should crash snowflake
    format_type_options = DEFAULT_FORMAT_TYPE_OPTIONS.copy() 
    format_type_options.pop('FIELD_OPTIONALLY_ENCLOSED_BY')

    with pytest.raises(ProgrammingError):
        # upload to snowflake
        with SnowflakeCsvWriter(
            csv_path=Path(__file__).parent.joinpath('./hwa_header.csv'),
            sfkl_name=TEST_TBL_NAME,
            sfkl_connector= SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) ,
            overwrite_tbl = True,
            format_type_options = format_type_options
        ) as sfkl_writer:
            sfkl_writer.create_table()

 
    teardown_ext_browser()

@pytest.mark.requires_remote_sfkl_interactivity
def test_snowflake_roundtrip_ext_browser():

    create_df_ext_browser(overwrite=True)

    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) as connector: 
        df_round_trip = pd.read_sql(f'SELECT * from {TEST_TBL_NAME}', con = connector.get_connection())

    assert df_round_trip.shape[0] == SAMPLE_DF.shape[0], 'Fatal error! Roundtrip data not read in as df with right number of rows!'
    assert df_round_trip.shape[1] == SAMPLE_DF.shape[1], 'Fatal error! Roundtrip data not read in as df with right number of columns!'
    
    # stage deleted
    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER) as connector:
        assert not stage_exists(connector.get_connection(), TEST_TBL_NAME)
        
    teardown_ext_browser()


@pytest.mark.requires_remote_sfkl_interactivity
def test_snowflake_overwrite_works():

    create_df_ext_browser(overwrite=True)

    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER).get_connection() as conn:
        df_num=pd.read_sql(  f'select count(*) NUM from {TEST_TBL_NAME}', conn )
        n_before = df_num.NUM.values[0]

    create_df_ext_browser(overwrite=True)

    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER).get_connection() as conn:
        df_num=pd.read_sql(  f'select count(*) NUM from {TEST_TBL_NAME}', conn )
        n_after = df_num.NUM.values[0]
    
    assert n_after == n_before, 'Fatal error! Overwriting fails'

    teardown_ext_browser()


@pytest.mark.requires_remote_sfkl_interactivity
def test_snowflake_append_works():

    create_df_ext_browser(overwrite=True)

    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER).get_connection() as conn:
        df_num=pd.read_sql(  f'select count(*) NUM from {TEST_TBL_NAME}', conn )
        n_before = df_num.NUM.values[0]

    create_df_ext_browser(overwrite=False)

    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER).get_connection() as conn:
        df_num=pd.read_sql(  f'select count(*) NUM from {TEST_TBL_NAME}', conn )
        n_after = df_num.NUM.values[0]
     
    assert n_after == SAMPLE_DF.shape[0] + SAMPLE_DF.shape[0], 'Fatal error! Appending fails'
    assert n_after > n_before
    
    teardown_ext_browser()

 


@pytest.mark.parametrize('custom_date,expected', [("2024-02-16 12:00:00 -0530", '12:30'), ("2024-06-16 12:00:00 -0530", '13:30')])
@pytest.mark.requires_remote_sfkl_interactivity
def test_datetime_formats_work_with_tz(custom_date, expected):

    # dropped buggy freezegun

    SAMPLE_DF_DATES = SAMPLE_DF.iloc[:1].copy(deep=True)
    SAMPLE_DF_DATES['datetime_tz_mtl_col'] = pd.to_datetime([custom_date],  utc=True).map(lambda x: x.tz_convert("America/Montreal"))

    with TempFile() as temp_csv:
        # Create the temp csv
        SAMPLE_DF_DATES.to_csv(temp_csv.name,index=False)

        with SnowflakeCsvWriter(
                    csv_path=temp_csv.name,
                    sfkl_name=TEST_TBL_NAME,
                    sfkl_connector=SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER),
                    overwrite_tbl=True
                ) as sfkl_writer:
                    
                sfkl_writer.create_table()

    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER).get_connection() as conn:
            df_round_trip = pd.read_sql(f'select * from {TEST_TBL_NAME}   ', conn)

    assert np.all(df_round_trip['DATETIME_TZ_MTL_COL'].dt.strftime('%H:%M').values == [expected])

    teardown_ext_browser()


@pytest.mark.requires_remote_sfkl_interactivity
def test_datetime_formats_work_no_tz():
 
    SAMPLE_DF_DATES = SAMPLE_DF.copy(deep=True)

    # timezones converted in sfkl using to_timestamp: uses TIMESTAMP_NTZ by default (so no timezone - ie as if all in the same local time)
    # 2024-02-16 12:00:00 -0530 means it is 12:00 locally in a place with an offset -5:30 from greenwich, so 17:30 +0000 UTC
    # Given standard time in summer, Mtl is UTC-4 so 1:30 ahead of UTC-5:30 (+5:30-4 = 1:30) so it is 13:30 locally (and still 17:30 +0000 UTC)
    # In the winter with DST Mtl is UTC-5 so it is only 0:30 ahead of the UTC-5:30. Given 12:00:00 -0530, it is 12:30 locally (and still 17:30 +0000 UTC)
    SAMPLE_DF_DATES['datetime_no_tz_col'] = pd.to_datetime(['2018-10-26 12:00', '2017-03-22 15:16:45.433502912'], format='mixed')
    SAMPLE_DF_DATES['datetime_tz_utc_col'] = pd.to_datetime(['2018-10-26 12:00 -0530', '2018-06-26 12:00 -0530'],  utc=True)
    SAMPLE_DF_DATES['timedelta_col'] = [pd.to_timedelta('1 days 06:05:01.00003'), pd.to_timedelta('4 days 06:05:01.00003')]
    

    LIST_DATETIME_COLS = ['datetime_no_tz_col'.upper(), 'datetime_tz_utc_col'.upper()]
    LIST_TIME_DELTA_COLS = ['timedelta_col'.upper()]

    # Dates treated naively without timezone (assumes same local time as sfkl)
    # Time delta not supported: so convert to string (easier to retrieve)
    EXPECTED_RESULTS = {
            **{v: "TIMESTAMP_NTZ" for v in LIST_DATETIME_COLS},
            **{v: "TEXT" for v in LIST_TIME_DELTA_COLS}
        }
    
    with TempFile() as temp_csv:
        # Create the temp csv
        SAMPLE_DF_DATES.to_csv(temp_csv.name,index=False)

        with SnowflakeCsvWriter(
                csv_path=temp_csv.name,
                sfkl_name=TEST_TBL_NAME,
                sfkl_connector=SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER),
                overwrite_tbl=True
            ) as sfkl_writer:
                
            sfkl_writer.create_table()

    with SnowflakeExternalBrowserConnection(PATH_CONFIG_EXT_BROWSER).get_connection() as conn:
        df_round_trip = pd.read_sql(f'select * from {TEST_TBL_NAME}   ', conn)
        df_cols=sfkl_show_column(conn, TEST_TBL_NAME)

    # Make sure types in sfkl are as expected: either TIMESTAMP_NTZ or TEXT
    for col, exp_type in EXPECTED_RESULTS.items() : 
        assert df_cols[df_cols['column_name'] == col]['json_col'].values[0]== exp_type
        
    # Make sure cols with timezones dealt with as expected
    assert np.all(df_round_trip['DATETIME_TZ_UTC_COL'].dt.strftime('%H:%M').values == ['17:30', '17:30' ])

    # Make sure we can read back timedeltas in pandas using to_timeselta
    for col in LIST_TIME_DELTA_COLS:
        assert str(pd.to_timedelta(df_round_trip[col]).dtype) == 'timedelta64[ns]'

    teardown_ext_browser()

if __name__ == '__main__':
    test_csv_upload_success_existing_stage()