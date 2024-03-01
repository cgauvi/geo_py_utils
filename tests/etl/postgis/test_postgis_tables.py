from os.path import abspath, dirname
import pytest
from pathlib import Path

from geo_py_utils.etl.postgis.db_utils import pg_create_extension
from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
from geo_py_utils.etl.postgis.postgis_tables import PostGISDBPublicTablesIdentifier, PostGISDBTablesIdentifier
from geo_py_utils.constants import ProjectPaths

#--
 
HERE = Path(abspath(dirname(__file__)))

# Make sure POSTGIS is minimally enabled!
@pytest.fixture
def connection_remote_prod_ro():
    pg_conn = PostGISDBConnection(ProjectPaths.CONFIG_PATH  / 'postgis' /'gcp' / "prod" / "ro.env")
    pg_create_extension(pg_conn.get_sql_alchemy_engine())
    return pg_conn
    
@pytest.fixture
def connection_remote_dev_admin():
    pg_conn = PostGISDBConnection(ProjectPaths.CONFIG_PATH  / 'postgis' /'gcp' / "dev" / ".env")
    pg_create_extension(pg_conn.get_sql_alchemy_engine())
    return pg_conn




@pytest.mark.requires_remote_pg_connection_dev
def test_list_public_tables_dev(connection_remote_dev_admin):

    pg_table_identifier = PostGISDBPublicTablesIdentifier(connection_remote_dev_admin)

    df_tables = pg_table_identifier.get_df_tables()

    assert df_tables.shape[0] > 0
    assert 'spatial_ref_sys' in df_tables.tablename.values


@pytest.mark.requires_remote_pg_connection_prod
def test_list_public_tables_prod(connection_remote_prod_ro):

    pg_table_identifier = PostGISDBPublicTablesIdentifier(connection_remote_prod_ro)

    df_tables = pg_table_identifier.get_df_tables()

    assert df_tables.shape[0] > 0
    assert 'spatial_ref_sys' in df_tables.tablename.values


@pytest.mark.requires_remote_pg_connection_prod
def test_list_public_geo_cols(connection_remote_prod_ro):

    pg_table_identifier = PostGISDBPublicTablesIdentifier(connection_remote_prod_ro)

    df_geo_cols = pg_table_identifier.get_df_geometry_columns()

    creds = pg_table_identifier.get_pg_connection().get_credentials()
    assert df_geo_cols.shape[1] > 0
    assert creds['database'] in df_geo_cols.f_table_catalog.values

@pytest.mark.requires_remote_pg_connection_prod
def test_list_topology_geo_cols(connection_remote_prod_ro):

    pg_table_identifier = PostGISDBTablesIdentifier(
        connection_remote_prod_ro, 
        selected_schemas=['topology']
        )

    df_geo_cols = pg_table_identifier.get_df_geometry_columns()

    assert df_geo_cols.shape[1]> 0



if __name__ == '__main__':

    test_list_public_tables_dev(
        PostGISDBConnection(HERE / 'config' / 'remote_pg_admin' / "dev" / ".env")
    )