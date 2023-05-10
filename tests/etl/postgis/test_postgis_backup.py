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
def connection_remote_admin():
    return PostGISDBConnection(HERE / 'config' / 'remote_pg_admin' / ".env")

 
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
def test_backup_to_gpkg(connection_remote_admin, dumy_gdf):

    tbl_dummy_name = 'dummy_table'

    # List public tables
    pg_list_public_tables = PostGISDBPublicTablesIdentifier(
        connection_remote_admin, 
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

    db_backup_creator.backup_to_destination()

    # Teardown
    # Drop table
    creds = pg_list_public_tables.get_pg_connection().get_credentials()
    with connect(**creds) as conn:
        cur = conn.cursor()
        cur.execute(sql.SQL('DROP TABLE IF EXISTS %s' % tbl_dummy_name))

    assert exists(HERE / "backuptest.gpkg")

    # Delete gpkg
    (HERE / "backuptest.gpkg").unlink()