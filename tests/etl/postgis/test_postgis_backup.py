from os.path import exists, abspath, dirname
import pytest
from pathlib import Path
import geopandas as gpd
from psycopg2 import sql, connect
from shapely.geometry import Polygon

from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
from geo_py_utils.etl.postgis.postgis_tables import PostGISDBPublicTablesIdentifier
from geo_py_utils.etl.postgis.postgis_backup import PostGISDBBackupGPK

#--
 
HERE = Path(abspath(dirname(__file__)))


@pytest.fixture
def connection_remote_admin_prod():
    return PostGISDBConnection(HERE / 'config' / 'remote_pg_admin' / "prod" / ".env")

@pytest.fixture
def connection_remote_admin_dev():
    return PostGISDBConnection(HERE / 'config' / 'remote_pg_admin' / "dev" / ".env")

 
@pytest.fixture
def dumy_gdf():

    shp = gpd.GeoDataFrame({'id': [0]},
                           geometry=[Polygon(((-71.5, 46.5),
                                              (-71.6, 46.4),
                                              (-71.5, 46.7),
                                              (-71.5, 46.5)))
                                     ],
                           crs=4326
                           )

    return shp 

@pytest.mark.requires_remote_pg_connection
def test_backup_to_gpkg(connection_remote_admin_prod, dumy_gdf):

    tbl_dummy_name = 'dummy_table'

    # List public tables
    pg_list_public_tables = PostGISDBPublicTablesIdentifier(
        connection_remote_admin_prod, 
        list_tables=[tbl_dummy_name]
    )

    # Load a dummy table
    engine = pg_list_public_tables.get_pg_connection().get_sql_alchemy_engine()
    with engine.connect() as conn:
        dumy_gdf.to_postgis(tbl_dummy_name, conn, if_exists='replace')

    # Backup selected tables to gpkg 
    db_backup_creator = PostGISDBBackupGPK(
        dest_gpkg = HERE / "backuptest.gpkg", 
        pg_tables_identifier = pg_list_public_tables,
        overwrite=True
    )

    db_backup_creator.postgis_to_gpkg()

    # Teardown
    # Drop table
    creds = pg_list_public_tables.get_pg_connection().get_credentials()
    with connect(**creds) as conn:
        cur = conn.cursor()
        cur.execute(sql.SQL('DROP TABLE IF EXISTS %s' % tbl_dummy_name))

    assert exists(HERE / "backuptest.gpkg")

    # Delete gpkg
    (HERE / "backuptest.gpkg").unlink()



@pytest.mark.requires_remote_pg_connection
def test_backup_to_gpkg_roundtrip(connection_remote_admin_prod, connection_remote_admin_dev, dumy_gdf):

    tbl_dummy_name = 'dummy_table'

    # List public tables
    pg_list_public_tables = PostGISDBPublicTablesIdentifier(
        connection_remote_admin_prod, 
        list_tables=[tbl_dummy_name]
    )

    # Load a dummy table
    engine = pg_list_public_tables.get_pg_connection().get_sql_alchemy_engine()
    with engine.connect() as conn:
        dumy_gdf.to_postgis(tbl_dummy_name, conn, if_exists='replace')

    # Backup selected tables to gpkg 
    db_backup_creator = PostGISDBBackupGPK(
        dest_gpkg = HERE / "backuptest.gpkg", 
        pg_tables_identifier = pg_list_public_tables,
        overwrite=True
    )

    db_backup_creator.postgis_to_gpkg()

    # Teardown
    # Drop table - prod
    creds = pg_list_public_tables.get_pg_connection().get_credentials()
    with connect(**creds) as conn:
        cur = conn.cursor()
        cur.execute(sql.SQL('DROP TABLE IF EXISTS %s' % tbl_dummy_name))

    assert exists(HERE / "backuptest.gpkg")

    # Reload back  to different DB
    db_backup_creator.gpkg_to_postgis(overwrite_pg_tbl =True)

    pg_list_public_tables_dev = PostGISDBPublicTablesIdentifier(
        connection_remote_admin_dev, 
        list_tables=[tbl_dummy_name]
    )
    assert tbl_dummy_name in pg_list_public_tables_dev.get_df_tables().tablename.values 

    # Drop table - dev
    creds = pg_list_public_tables_dev.get_pg_connection().get_credentials()
    with connect(**creds) as conn:
        cur = conn.cursor()
        cur.execute(sql.SQL('DROP TABLE IF EXISTS %s' % tbl_dummy_name))

    # Delete gpkg
    (HERE / "backuptest.gpkg").unlink()