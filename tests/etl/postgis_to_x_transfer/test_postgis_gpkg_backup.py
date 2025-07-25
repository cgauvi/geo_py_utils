# Some of these tests were run with the following pytest commands:

# E.g. to test `dev`

# `pytest tests/etl/postgis/test_postgis_backup.py \
# -k 'test_backup_to_gpkg_dev_roundtrip'

# `pytest tests/etl/postgis/ \
# -m 'requires_remote_pg_connection_dev and not requires_remote_pg_connection_prod' 
 

from os.path import exists, abspath, dirname
import pytest
from pathlib import Path
from psycopg2 import sql, connect

from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
from geo_py_utils.etl.postgis.postgis_tables import PostGISDBTablesIdentifier
from geo_py_utils.etl.postgis.postgis_backup import PostGISDBBackupGPK
from geo_py_utils.constants import ProjectPaths
from geo_py_utils.utilities_testing.data import SAMPLE_GDF_SAFE

#--
 
HERE = Path(abspath(dirname(__file__)))


@pytest.fixture
def connection_remote_admin_prod():
    return PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / "aws" / "prod" / ".env")

@pytest.fixture
def connection_remote_admin_dev():
    return PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / "aws" /  "dev" / ".env")

  
@pytest.mark.requires_remote_pg_connection_dev
def test_backup_to_gpkg(connection_remote_admin_dev):

    tbl_dummy_name = 'dummy_table'

    # List public tables
    pg_list_tables = PostGISDBTablesIdentifier(
        connection_remote_admin_dev, 
        selected_schemas=connection_remote_admin_dev.default_schema,
        selected_tables=[tbl_dummy_name]
    )

    # Load a dummy table
    engine = pg_list_tables.get_pg_connection().get_sql_alchemy_engine()
    with engine.connect() as conn:
        SAMPLE_GDF_SAFE.to_postgis(tbl_dummy_name, conn, schema = connection_remote_admin_dev.default_schema, if_exists='replace')

    # Backup selected tables to gpkg 
    db_backup_creator = PostGISDBBackupGPK(
        dest_gpkg = HERE / "backuptest.gpkg", 
        pg_tables_identifier = pg_list_tables,
        overwrite=True
    )

    db_backup_creator.postgis_to_gpkg()

    # Teardown
    # Drop table
    creds = pg_list_tables.get_pg_connection().get_credentials()
    with connect(**creds) as conn:
        cur = conn.cursor()
        cur.execute(sql.SQL('DROP TABLE IF EXISTS %s' % tbl_dummy_name))

    assert exists(HERE / "backuptest.gpkg")

    # Delete gpkg
    (HERE / "backuptest.gpkg").unlink()



@pytest.mark.requires_remote_pg_connection_prod_dev
def test_backup_to_gpkg_dev_prod(connection_remote_admin_prod, connection_remote_admin_dev):

    tbl_dummy_name = 'dummy_table'

    # List tables
    pg_list_tables = PostGISDBTablesIdentifier(
        connection_remote_admin_prod, 
        selected_schemas=connection_remote_admin_prod.default_schema,
        selected_tables=[tbl_dummy_name]
    )

    # Load a dummy table
    engine = pg_list_tables.get_pg_connection().get_sql_alchemy_engine()
    with engine.begin() as conn:
        SAMPLE_GDF_SAFE.to_postgis(tbl_dummy_name, conn, schema = connection_remote_admin_prod.default_schema, if_exists='replace')

    # Backup selected tables to gpkg 
    db_backup_creator = PostGISDBBackupGPK(
        dest_gpkg = f'"{HERE / "backuptest.gpkg"}"', 
        pg_tables_identifier = pg_list_tables,
        overwrite=True
    )

    db_backup_creator.postgis_to_gpkg()

    # Teardown
    # Drop table - prod
    creds = pg_list_tables.get_pg_connection().get_credentials()
    with connect(**creds) as conn:
        cur = conn.cursor()
        cur.execute(sql.SQL('DROP TABLE IF EXISTS %s' % tbl_dummy_name))

    assert exists(HERE / "backuptest.gpkg")

    # Reload back  to different DB
    db_backup_creator.gpkg_to_postgis(connection_remote_admin_dev, overwrite_pg_tbl =True)

    pg_list_tables_dev = PostGISDBTablesIdentifier(
        connection_remote_admin_dev, 
        selected_schemas=connection_remote_admin_dev.default_schema,
        selected_tables=[tbl_dummy_name]
    )
    assert tbl_dummy_name in pg_list_tables_dev.get_df_tables().tablename.values 

    # Drop table - dev
    creds = pg_list_tables_dev.get_pg_connection().get_credentials()
    with connect(**creds) as conn:
        cur = conn.cursor()
        cur.execute(sql.SQL('DROP TABLE IF EXISTS %s' % tbl_dummy_name))

    # Delete gpkg
    (HERE / "backuptest.gpkg").unlink()


@pytest.mark.requires_remote_pg_connection_prod
def test_backup_to_gpkg_prod_roundtrip(connection_remote_admin_prod):

    tbl_dummy_name = 'dummy_table'

    # List public tables
    pg_list_tables = PostGISDBTablesIdentifier(
        connection_remote_admin_prod, 
        selected_schemas=connection_remote_admin_prod.default_schema,
        selected_tables=[tbl_dummy_name]
    )

    # Load a dummy table
    engine = pg_list_tables.get_pg_connection().get_sql_alchemy_engine()
    with engine.connect() as conn:
        SAMPLE_GDF_SAFE.to_postgis(tbl_dummy_name, conn, schema = connection_remote_admin_prod.default_schema, if_exists='replace')

    # Backup selected tables to gpkg 
    db_backup_creator = PostGISDBBackupGPK(
        dest_gpkg = f'"{HERE / "backuptest.gpkg"}"', 
        pg_tables_identifier = pg_list_tables,
        overwrite=True
    )

    db_backup_creator.postgis_to_gpkg()

    # Teardown
    # Drop table - prod
    creds = pg_list_tables.get_pg_connection().get_credentials()
    with connect(**creds) as conn:
        cur = conn.cursor()
        cur.execute(sql.SQL('DROP TABLE IF EXISTS %s' % tbl_dummy_name))

    assert exists(HERE / "backuptest.gpkg")

    # Reload back  to SAME DB
    db_backup_creator.gpkg_to_postgis(overwrite_pg_tbl = False)
 
    assert tbl_dummy_name in pg_list_tables.get_df_tables().tablename.values 

    # Drop table - dev
    creds = pg_list_tables.get_pg_connection().get_credentials()
    with connect(**creds) as conn:
        cur = conn.cursor()
        cur.execute(sql.SQL('DROP TABLE IF EXISTS %s' % tbl_dummy_name))

    # Delete gpkg
    (HERE / "backuptest.gpkg").unlink()


 


@pytest.mark.requires_remote_pg_connection_dev
def test_backup_to_gpkg_dev_roundtrip(connection_remote_admin_dev):

    tbl_dummy_name = 'dummy_table'

    # List public tables
    pg_list_tables = PostGISDBTablesIdentifier(
        connection_remote_admin_dev, 
        selected_schemas=connection_remote_admin_dev.default_schema,
        selected_tables=[tbl_dummy_name]
    )

    # Load a dummy table
    engine = pg_list_tables.get_pg_connection().get_sql_alchemy_engine()
    with engine.connect() as conn:
        SAMPLE_GDF_SAFE.to_postgis(tbl_dummy_name, conn, schema = connection_remote_admin_dev.default_schema, if_exists='replace')

    # Backup selected tables to gpkg 
    db_backup_creator = PostGISDBBackupGPK(
        dest_gpkg = f'"{HERE / "backuptest.gpkg"}"', 
        pg_tables_identifier = pg_list_tables,
        overwrite=True
    )

    db_backup_creator.postgis_to_gpkg()

    # Teardown
    # Drop table - prod
    creds = pg_list_tables.get_pg_connection().get_credentials()
    with connect(**creds) as conn:
        cur = conn.cursor()
        cur.execute(sql.SQL('DROP TABLE IF EXISTS %s' % tbl_dummy_name))

    assert exists(HERE / "backuptest.gpkg")

    # Reload back  to SAME DB
    db_backup_creator.gpkg_to_postgis(overwrite_pg_tbl = False)
 
    assert tbl_dummy_name in pg_list_tables.get_df_tables().tablename.values 

    # Drop table - dev
    creds = pg_list_tables.get_pg_connection().get_credentials()
    with connect(**creds) as conn:
        cur = conn.cursor()
        cur.execute(sql.SQL('DROP TABLE IF EXISTS %s' % tbl_dummy_name))

    # Delete gpkg
    (HERE / "backuptest.gpkg").unlink()


if __name__ == '__main__':

    test_backup_to_gpkg_dev_prod(
        PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / "aws" / "prod" / ".env"),
        PostGISDBConnection(ProjectPaths.CONFIG_PATH / 'postgis' / "aws" /  "dev" / ".env")
    )