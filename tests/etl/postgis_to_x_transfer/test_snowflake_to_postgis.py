import geopandas as gpd
import pandas as pd
import pytest

from geo_py_utils.etl.snowflake.snowflake_ext_browser_connection import SnowflakeExternalBrowserConnection
from geo_py_utils.etl.snowflake.gdf_write import SnowflakeGeoWriter
from geo_py_utils.etl.snowflake.gdf_load import sfkl_to_gpd
from geo_py_utils.etl.snowflake.db_utils import sfkl_get_table_srid
from geo_py_utils.etl.postgis.load_sfkl_to_postgis import LoadSfklPostgis
from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection
from geo_py_utils.constants import ProjectPaths
from geo_py_utils.utilities_testing.data import SAMPLE_GDF

TEST_SFKL_TBL_NAME = 'TEST_SHP_WRITE_SFKL_NEIGH'
PATH_SFKL_CONFIG = ProjectPaths.CONFIG_PATH / 'snowflake' / 'extbrowser' / ".env"
PATH_POSTGIS_CONFIG = ProjectPaths.CONFIG_PATH / 'postgis' / 'gcp' / 'dev' / ".env"
SFKL_GEO_COL_NAME='GEOGRAPHY'

def create_gdf():

    with SnowflakeGeoWriter(
        shp=SAMPLE_GDF,
        sfkl_name=TEST_SFKL_TBL_NAME,
        sfkl_connector= SnowflakeExternalBrowserConnection(PATH_SFKL_CONFIG) ,
        sfkl_geo_col_name=SFKL_GEO_COL_NAME
    ) as sfkl_writer:
        
        sfkl_writer.create_table()
 
@pytest.mark.requires_remote_pg_connection_sfkl_interactivity
def test_transfer_sfkl_postgis():

    create_gdf()

    pg_db_conn = PostGISDBConnection(PATH_POSTGIS_CONFIG)
    creds_dict = pg_db_conn.get_credentials()
 
    sfkl_connector = SnowflakeExternalBrowserConnection(PATH_SFKL_CONFIG)

    db_transfer = LoadSfklPostgis(
        sfkl_tbl_name=TEST_SFKL_TBL_NAME,
        sfkl_connector=sfkl_connector, 
        host = creds_dict['host'],
        user= creds_dict['user'],
        password= creds_dict['password'],
        port= creds_dict['port'],
        postgis_tbl_name=TEST_SFKL_TBL_NAME
    )

    db_transfer.upload_url_to_database()

    query_count = f'select count(*) NUM_ROWS from {TEST_SFKL_TBL_NAME}'
    query_get_all = f'select * from {TEST_SFKL_TBL_NAME}'

    # Read from postgis
    with pg_db_conn.get_sql_alchemy_engine().connect() as conn:
        df_pg = pd.read_sql(query_count, conn)
        shp_pg = gpd.read_postgis(query_get_all, conn, geom_col='geom') # default postgis ..
        df_sfkl = pd.read_sql(query_count, conn)

    # Read from snowflake
    with sfkl_connector as connector:
        crs = sfkl_get_table_srid(connector.get_connection(), TEST_SFKL_TBL_NAME, SFKL_GEO_COL_NAME)

        shp_sfkl = sfkl_to_gpd(
            query_get_all, 
            connector.get_connection(), 
            crs=crs,
            geometry_name=SFKL_GEO_COL_NAME
        )

    
    assert shp_pg.shape[0] == shp_sfkl.shape[0]  , \
        f'Fatal error! dimension {0} mismatch - {shp_pg.shape[0]} vs {shp_sfkl.shape[0]}'
    
    # PG adds a fid column
    assert shp_pg.shape[1] -1  == shp_sfkl.shape[1] , \
            f'Fatal error! dimension {1} mismatch - {shp_pg.shape[1]} vs {shp_sfkl.shape[1]}'

    assert df_sfkl.num_rows.values[0] == df_pg.num_rows.values[0], 'Fatal errro! Count different'



 
        
if __name__ == '__main__':

    test_transfer_sfkl_postgis()