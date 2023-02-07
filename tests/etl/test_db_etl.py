

import subprocess
from os.path import join
import geopandas as gpd
import os
import tempfile

from geo_py_utils.etl.db_etl import  Url_to_spatialite, Url_to_postgis   
from geo_py_utils.etl.gdf_load import spatialite_db_to_gdf
from geo_py_utils.etl.db_utils import list_tables, sql_to_df, get_table_rows, get_table_crs, drop_table, is_spatial_index_enabled
from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
from geo_py_utils.census_open_data.census import FSA_2016_URL


class Qc_city_data:
    SPATIAL_LITE_DB_PATH = join(DATA_DIR, "test.db")
    SPATIAL_LITE_TBL_QC = "qc_city_test_tbl"
    QC_CITY_NEIGH_URL = QC_CITY_NEIGH_URL


def upload_qc_neigh_db(db_name = Qc_city_data.SPATIAL_LITE_DB_PATH,
                        tbl_name = Qc_city_data.SPATIAL_LITE_TBL_QC,
                        download_url = Qc_city_data.QC_CITY_NEIGH_URL):
    
    with Url_to_spatialite(
        db_name = db_name, 
        table_name = tbl_name,
        download_url = download_url,
        download_destination = DATA_DIR) as uploader:

        uploader.upload_url_to_database()


def test_spatial_index():

    if (not os.path.exists(Qc_city_data.SPATIAL_LITE_DB_PATH)) or \
        (not Qc_city_data.SPATIAL_LITE_TBL_QC in list_tables(Qc_city_data.SPATIAL_LITE_DB_PATH)) :
        upload_qc_neigh_db()

    assert is_spatial_index_enabled(
        Qc_city_data.SPATIAL_LITE_DB_PATH,
        Qc_city_data.SPATIAL_LITE_TBL_QC
        )

def test_upload_spatialite_qc():
    upload_qc_neigh_db()



def test_read_gdf_spatialite_qc ():

    if not os.path.exists(Qc_city_data.SPATIAL_LITE_DB_PATH):
        upload_qc_neigh_db()

    df = spatialite_db_to_gdf(db_name = Qc_city_data.SPATIAL_LITE_DB_PATH,
                            tbl_name = Qc_city_data.SPATIAL_LITE_TBL_QC,
                            additional_where_limit_causes = "LIMIT 10"
    ) 

    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] == 10


def test_spatialite_list_tables():

    if not os.path.exists(Qc_city_data.SPATIAL_LITE_DB_PATH):
        upload_qc_neigh_db()

    existing_tables = list_tables(Qc_city_data.SPATIAL_LITE_DB_PATH)

    assert Qc_city_data.SPATIAL_LITE_TBL_QC in existing_tables


def test_spatialite_send_query():
    df_results = sql_to_df(Qc_city_data.SPATIAL_LITE_DB_PATH, 
                            f"select count(*) as num_rows from {Qc_city_data.SPATIAL_LITE_TBL_QC}")

    assert get_table_rows(Qc_city_data.SPATIAL_LITE_DB_PATH, Qc_city_data.SPATIAL_LITE_TBL_QC) == df_results.num_rows.values[0]



def test_spatialite_zip_with_proj():

    with Url_to_spatialite(
        db_name = join(DATA_DIR, "test_fsa.db"), 
        table_name = 'geo_fsa_tbl',
        download_url = FSA_2016_URL,
        download_destination = DATA_DIR,
        target_projection= 32198,
        overwrite=True) as uploader:

        uploader.upload_url_to_database()

    #Load back into memory using geopandas
    shp_test = spatialite_db_to_gdf(join(DATA_DIR, "test_fsa.db"),
     'geo_fsa_tbl',
     'limit 10'
     )

    assert shp_test.crs == 32198

    # Get the crs using spatialite only
    crs_spatial_lite = get_table_crs(join(DATA_DIR, "test_fsa.db"), 'geo_fsa_tbl', return_srid = True)
    assert crs_spatial_lite == 32198

def test_spatialite_local_file():

    # Remove if existing
    if os.path.exists(Qc_city_data.SPATIAL_LITE_DB_PATH):
        os.remove(Qc_city_data.SPATIAL_LITE_DB_PATH)

    # Read the shp file from the url + write to disk before uploading to spatialite
    shp_qc = gpd.read_file(QC_CITY_NEIGH_URL)
    tmp_dir = tempfile.mkdtemp()
    path_shp_file_local = join(tmp_dir, 'tmp.shp')
    shp_qc.to_file(path_shp_file_local)

    with Url_to_spatialite(
                        db_name = Qc_city_data.SPATIAL_LITE_DB_PATH, 
                        table_name = Qc_city_data.SPATIAL_LITE_TBL_QC,
                        download_url = path_shp_file_local, # local file does not require curl -- hacky + watch out, Url_to_spatialite will automatically delete the file 
                        overwrite = True #for some weird reason, appending duplicates the dataset -- even if we start of with a clean db
        ) as spatialite_etl:

        # Upload the point DB 
        spatialite_etl.upload_url_to_database()

    
    shp_test = spatialite_db_to_gdf( Qc_city_data.SPATIAL_LITE_DB_PATH,
     Qc_city_data.SPATIAL_LITE_TBL_QC
     )

    assert shp_qc.shape[0] == shp_test.shape[0]
 

def test_drop_table():

     # Make sure table exists initially
    if not os.path.exists(Qc_city_data.SPATIAL_LITE_DB_PATH):
        upload_qc_neigh_db()

    # Drop table 
    drop_table(Qc_city_data.SPATIAL_LITE_DB_PATH, Qc_city_data.SPATIAL_LITE_TBL_QC)

    # Table not present
    assert Qc_city_data.SPATIAL_LITE_TBL_QC not in list_tables(Qc_city_data.SPATIAL_LITE_DB_PATH)
 

    

def test_postgis_qc():

    import subprocess
    import logging

    logger = logging.getLogger(__name__)

    psql_exists_results = subprocess.run(["which","psql"], stdout=subprocess.PIPE)
    if len(psql_exists_results.stdout) > 0:

        user = os.getenv('PG_LOCAL_USER') # defaults to None if not set
        password = os.getenv('PG_LOCAL_PASSWORD')
        postgis_db_path = 'qc_city_db'

        with Url_to_postgis(
            db_name = postgis_db_path, 
            table_name = "qc_city_test_tbl",
            download_url = QC_CITY_NEIGH_URL,
            download_destination = DATA_DIR, 
            host = "localhost",
            port = 5432,
            user = user, 
            password = password,
            schema = "public",
            ) as uploader:

            if uploader._port_is_open():
                uploader.upload_url_to_database()
            else:
                logger.warning("Skipping test_postgis: port seems closed")
    else:
        logger.warning("Skipping test_postgis: did not find psql on system")

 

if __name__ == '__main__':

    test_spatial_index()