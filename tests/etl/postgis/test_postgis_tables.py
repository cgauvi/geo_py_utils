from os.path import abspath, dirname
import pytest
from pathlib import Path

from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
from geo_py_utils.etl.postgis.postgis_tables import PostGISDBPublicTablesIdentifier

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
