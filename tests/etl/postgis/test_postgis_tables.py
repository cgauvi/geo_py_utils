from os.path import abspath, dirname
import pytest
from pathlib import Path

from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
from geo_py_utils.etl.postgis.postgis_tables import PostGISDBPublicTablesIdentifier, PostGISDBTablesIdentifier

#--
 
HERE = Path(abspath(dirname(__file__)))


@pytest.fixture
def connection_remote_ro():
    return PostGISDBConnection(HERE / 'config' / 'remote_pg_ro' / ".env")


@pytest.mark.requires_remote_pg_connection_prod
def test_list_public_tables(connection_remote_ro):

    pg_table_identifier = PostGISDBPublicTablesIdentifier(connection_remote_ro)

    df_tables = pg_table_identifier.get_df_tables()

    assert df_tables.shape[0] > 0
    assert 'spatial_ref_sys' in df_tables.tablename.values


@pytest.mark.requires_remote_pg_connection_prod
def test_list_public_geo_cols(connection_remote_ro):

    pg_table_identifier = PostGISDBPublicTablesIdentifier(connection_remote_ro)

    df_geo_cols = pg_table_identifier.get_df_geometry_columns()

    creds = pg_table_identifier.get_pg_connection().get_credentials()
    assert df_geo_cols.shape[0] > 0
    assert creds['database'] in df_geo_cols.f_table_catalog.values

@pytest.mark.requires_remote_pg_connection_prod
def test_list_cron_geo_cols(connection_remote_ro):

    pg_table_identifier = PostGISDBTablesIdentifier(
        connection_remote_ro, 
        selected_schemas=['cron']
        )

    df_geo_cols = pg_table_identifier.get_df_geometry_columns()

    assert df_geo_cols.shape[0] == 0
 