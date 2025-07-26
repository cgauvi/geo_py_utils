# Some of these tests were run with the following pytest commands:

# E.g. to test `dev`

# `pytest tests/etl/postgis_to_x_transfer/test_postgis_postgis.py \
# -k 'test_new_table_transfer_aws_dev_to_aws_prod_succeeds'
# -m requires_remote_pg_connection_dev

# `pytest tests/etl/postgis/ \
# -m 'requires_remote_pg_connection_dev and not requires_remote_pg_connection_prod' 
 

import pytest
import pandas as pd
from sqlalchemy import text

from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
from geo_py_utils.constants import ProjectPaths
from geo_py_utils.etl.postgis.load_postgis_to_postgis import LoadPostgis2Postgis
from geo_py_utils.utilities_testing.data import SAMPLE_GDF_SAFE, SAMPLE_GDF_2, GDF_BBOX_DONNACONNA
from geo_py_utils.etl.postgis.db_utils import pg_list_tables

TBL_DUMMY_NAME= 'dummy_table'
TBL_DUMMY_NAME_2= 'dummy_table_2'
TBL_DUMMY_NAME_3 = 'dumy_table_3'

TBLS={
    TBL_DUMMY_NAME: SAMPLE_GDF_SAFE,
    TBL_DUMMY_NAME_2: SAMPLE_GDF_2,
    TBL_DUMMY_NAME_3: GDF_BBOX_DONNACONNA
}

@pytest.fixture
def connection_remote_aws_prod(p=ProjectPaths.CONFIG_PATH / 'postgis' / "aws" / "prod" / ".env"):

    connector = PostGISDBConnection(p)

    # Load a dummy table
    with connector.get_sql_alchemy_engine().begin()  as conn:
        for tbl_name, gdf in TBLS.items():
            gdf.to_postgis(tbl_name, conn, if_exists='replace', schema=connector.default_schema)

    yield connector

    # Teardown
    with connector.get_sql_alchemy_engine().begin()  as conn:
        for tbl_name, gdf in TBLS.items():
            conn.execute(text(f'DROP TABLE IF EXISTS {tbl_name}'))


@pytest.fixture
def connection_remote_aws_dev(p=ProjectPaths.CONFIG_PATH / 'postgis' / "aws" / "dev" / ".env"):

    connector = PostGISDBConnection(p)

    # Clean slate
    with connector.get_sql_alchemy_engine().begin()  as conn:
        for tbl_name, _ in TBLS.items():
            conn.execute(text(f'DROP TABLE IF EXISTS {tbl_name}'))

    yield connector

    # Teardown
    with connector.get_sql_alchemy_engine().begin()  as conn:
        for tbl_name, _ in TBLS.items():
            conn.execute(text(f'DROP TABLE IF EXISTS {tbl_name}'))
  

@pytest.mark.requires_remote_pg_connection_prod_dev
def test_new_table_transfer_aws_dev_to_aws_prod_succeeds(connection_remote_aws_dev, connection_remote_aws_prod):

    list_dest_before = pg_list_tables(
        connection_remote_aws_prod.get_sql_alchemy_engine(),
        connection_remote_aws_prod.creds['database'] 
    )

    # LoadPostgis2Postgis takes in arguments like dest_port and src_host 
    # However PostGISDBConnection returns a dict with keys port, host, db, etc. 
    # So we need to append an extra src or dest prefix to kwargs
    creds = {}
    creds_new = {}
    for k,o in [ (connection_remote_aws_prod ,'dest'), (connection_remote_aws_dev, 'src')]:
        creds[o] = k.get_credentials().copy()
        creds_new[o] = {}
        for kk,v in creds[o].items():
            creds_new[o][f'{o}_{kk}'] = v

    all_params = creds_new['dest'].copy()
    all_params.update(creds_new['src'].copy())

    # Schemas
    all_params.update(
    {
            'dest_default_schema': connection_remote_aws_prod.default_schema,
            'src_default_schema': connection_remote_aws_dev.default_schema,
    })
    
    # Transfer the table 
    loader = LoadPostgis2Postgis(
        tables_to_transfer= TBL_DUMMY_NAME,
        **all_params
    )

    loader.upload_url_to_database()


    # Make sure the new table is there 
    with connection_remote_aws_prod.get_sql_alchemy_engine().begin()  as conn:
        df_count = pd.read_sql(f'select count(*) num from {TBL_DUMMY_NAME}', conn)
    assert df_count['num'].values > 0 

    # Make sure we only added that table 
    list_dest_after = pg_list_tables(
        connection_remote_aws_prod.get_sql_alchemy_engine(),
        connection_remote_aws_prod.creds['database'] 
    )

    assert len(list_dest_after) == len(list_dest_before) + 1



@pytest.mark.requires_remote_pg_connection_prod_dev
def test_append_table_transfer_aws_dev_to_aws_prod_succeeds(connection_remote_aws_dev, connection_remote_aws_prod):


    # LoadPostgis2Postgis takes in arguments like dest_port and src_host 
    # However PostGISDBConnection returns a dict with keys port, host, db, etc. 
    # So we need to append an extra src or dest prefix to kwargs
    creds = {}
    creds_new = {}
    for k,o in [ (connection_remote_aws_prod ,'dest'), (connection_remote_aws_dev, 'src')]:
        creds[o] = k.get_credentials().copy()
        creds_new[o] = {}
        for kk,v in creds[o].items():
            creds_new[o][f'{o}_{kk}'] = v

    all_params = creds_new['dest'].copy()
    all_params.update(creds_new['src'].copy())

    # Transfer the table 
    loader = LoadPostgis2Postgis(
        tables_to_transfer= TBL_DUMMY_NAME,
        **all_params
    )

    loader.upload_url_to_database()

    # Re-transfer the table 
    reloader = LoadPostgis2Postgis(
        tables_to_transfer= TBL_DUMMY_NAME,
        append=True,
        **all_params
    )

    reloader.upload_url_to_database()

    # Make sure the new table is there and has twice the number of rows
    rows_before = TBLS[TBL_DUMMY_NAME].shape[0]
    with connection_remote_aws_prod.get_sql_alchemy_engine().begin()  as conn:
        df_count = pd.read_sql(f'select count(*) num from {TBL_DUMMY_NAME}', conn)

    rows_after = df_count['num'].values[0]
    assert rows_after == 2*rows_before



@pytest.mark.requires_remote_pg_connection_dev
def test_overwrite_all_tables_transfer_aws_dev_to_aws_dev_succeeds(connection_remote_aws_dev, connection_remote_aws_prod):

    
    # LoadPostgis2Postgis takes in arguments like dest_port and src_host 
    # However PostGISDBConnection returns a dict with keys port, host, db, etc. 
    # So we need to append an extra src or dest prefix to kwargs
    creds = {}
    creds_new = {}
    for k,o in [ (connection_remote_aws_prod,'dest'), (connection_remote_aws_dev, 'src')]:
        creds[o] = k.get_credentials().copy()
        creds_new[o] = {}
        for kk,v in creds[o].items():
            creds_new[o][f'{o}_{kk}'] = v

    all_params = creds_new['dest'].copy()
    all_params.update(creds_new['src'].copy())

 

    # Transfer ALL tables 
    loader = LoadPostgis2Postgis(
        **all_params
    )
    tables_src = loader.tables_to_transfer
    loader.upload_url_to_database()

    # Retransfer ALL tables 
    reloader = LoadPostgis2Postgis(
        overwrite=True,
        **all_params
    )
    reloader.upload_url_to_database()

    tables_dest = pg_list_tables(
        connection_remote_aws_prod.get_sql_alchemy_engine(), 
        connection_remote_aws_prod.get_credentials()['database'],
        connection_remote_aws_prod.default_schema
    )

    # Make sure each table in the src was copied to the dest 
    assert set(tables_src ).issubset(tables_dest)


    # Make sure the sample table has the same number of rows
    rows_before = TBLS[TBL_DUMMY_NAME].shape[0]
    with connection_remote_aws_prod.get_sql_alchemy_engine().begin()  as conn:
        df_count = pd.read_sql(f'select count(*) num from {TBL_DUMMY_NAME}', conn)

    rows_after = df_count['num'].values[0]
    assert rows_after == rows_before

    
if __name__ == '__main__':

    test_new_table_transfer_aws_dev_to_aws_prod_succeeds(
        PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / "aws" / "dev" / ".env"),
        PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / "aws" / "prod" / ".env")
    )