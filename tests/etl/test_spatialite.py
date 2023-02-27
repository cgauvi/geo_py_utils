from os.path import join
import geopandas as gpd
import os
import tempfile
import numpy as np
import pytest

from geo_py_utils.etl.db_etl import  Url_to_spatialite
from geo_py_utils.etl.spatialite.gdf_load import spatialite_db_to_gdf
from geo_py_utils.etl.spatialite.db_utils import (
    list_tables, 
    sql_to_df, 
    get_table_rows,
    get_table_crs,
    drop_table,
    drop_geo_table_all,
    is_spatial_index_enabled,
    is_spatial_index_enabled_valid,
    get_table_geometry_type,
    promote_multi
)
from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
from geo_py_utils.census_open_data.census import FSA_2016_URL
from geo_py_utils.etl.spatialite.utils_testing import upload_qc_neigh_test_db, QcCityTestData



def test_promot_to_multi():

    # Make sure we start from scratch, and dont defacto promot to multipolygon
    drop_geo_table_all(QcCityTestData.SPATIAL_LITE_DB_PATH, QcCityTestData.SPATIAL_LITE_TBL_QC, 'GEOMETRY')
    if (not os.path.exists(QcCityTestData.SPATIAL_LITE_DB_PATH)) or \
        (not QcCityTestData.SPATIAL_LITE_TBL_QC in list_tables(QcCityTestData.SPATIAL_LITE_DB_PATH)) :
        upload_qc_neigh_test_db(promote_to_multi=False)

    shp_before = spatialite_db_to_gdf(QcCityTestData.SPATIAL_LITE_DB_PATH, QcCityTestData.SPATIAL_LITE_TBL_QC)
    
    promote_multi(QcCityTestData.SPATIAL_LITE_DB_PATH, QcCityTestData.SPATIAL_LITE_TBL_QC)

    shp_after = spatialite_db_to_gdf(QcCityTestData.SPATIAL_LITE_DB_PATH, QcCityTestData.SPATIAL_LITE_TBL_QC)

    # The geometry is not properly set in the aux geometry_columns table, but the geometry might still be successfully changed
    assert len(shp_before.geometry.type.unique()) == 2

    assert len(shp_after.geometry.type.unique()) == 1
    assert shp_after.geometry.type.unique() == 'MultiPolygon' 


def test_geometry_type():
    if (not os.path.exists(QcCityTestData.SPATIAL_LITE_DB_PATH)) or \
        (not QcCityTestData.SPATIAL_LITE_TBL_QC in list_tables(QcCityTestData.SPATIAL_LITE_DB_PATH)) :
        upload_qc_neigh_test_db()

    with pytest.raises(Exception):
        get_table_geometry_type( 
            QcCityTestData.SPATIAL_LITE_DB_PATH,
            QcCityTestData.SPATIAL_LITE_TBL_QC) == 'POLYGON' # We expect a polygon, but the geometry type is mixed so we get an error

def test_spatial_index_enabled():

    if (not os.path.exists(QcCityTestData.SPATIAL_LITE_DB_PATH)) or \
        (not QcCityTestData.SPATIAL_LITE_TBL_QC in list_tables(QcCityTestData.SPATIAL_LITE_DB_PATH)) :
        upload_qc_neigh_test_db()

    assert is_spatial_index_enabled(
        QcCityTestData.SPATIAL_LITE_DB_PATH,
        QcCityTestData.SPATIAL_LITE_TBL_QC
        )

    

def test_spatial_index_valid():

    if (not os.path.exists(QcCityTestData.SPATIAL_LITE_DB_PATH)) or \
        (not QcCityTestData.SPATIAL_LITE_TBL_QC in list_tables(QcCityTestData.SPATIAL_LITE_DB_PATH)) :
        upload_qc_neigh_test_db()

    assert is_spatial_index_enabled_valid(
        QcCityTestData.SPATIAL_LITE_DB_PATH,
        QcCityTestData.SPATIAL_LITE_TBL_QC,
        QcCityTestData.SPATIAL_LITE_TBL_GEOMETRY_COL_NAME
        )


def test_upload_spatialite_qc():
    upload_qc_neigh_test_db()



def test_read_gdf_spatialite_qc ():

    if not os.path.exists(QcCityTestData.SPATIAL_LITE_DB_PATH):
        upload_qc_neigh_test_db()

    df = spatialite_db_to_gdf(db_name = QcCityTestData.SPATIAL_LITE_DB_PATH,
                            tbl_name = QcCityTestData.SPATIAL_LITE_TBL_QC,
                            additional_where_limit_causes = "LIMIT 10"
    ) 

    assert isinstance(df, gpd.GeoDataFrame)
    assert df.shape[0] == 10


def test_spatialite_list_tables():

    if not os.path.exists(QcCityTestData.SPATIAL_LITE_DB_PATH):
        upload_qc_neigh_test_db()

    existing_tables = list_tables(QcCityTestData.SPATIAL_LITE_DB_PATH)

    assert QcCityTestData.SPATIAL_LITE_TBL_QC in existing_tables


def test_spatialite_send_query():
    df_results = sql_to_df(QcCityTestData.SPATIAL_LITE_DB_PATH, 
                            f"select count(*) as num_rows from {QcCityTestData.SPATIAL_LITE_TBL_QC}")

    assert get_table_rows(QcCityTestData.SPATIAL_LITE_DB_PATH, QcCityTestData.SPATIAL_LITE_TBL_QC) == df_results.num_rows.values[0]



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
    if os.path.exists(QcCityTestData.SPATIAL_LITE_DB_PATH):
        os.remove(QcCityTestData.SPATIAL_LITE_DB_PATH)

    # Read the shp file from the url + write to disk before uploading to spatialite
    shp_qc = gpd.read_file(QC_CITY_NEIGH_URL)
    tmp_dir = tempfile.mkdtemp()
    path_shp_file_local = join(tmp_dir, 'tmp.shp')
    shp_qc.to_file(path_shp_file_local)

    with Url_to_spatialite(
                        db_name = QcCityTestData.SPATIAL_LITE_DB_PATH, 
                        table_name = QcCityTestData.SPATIAL_LITE_TBL_QC,
                        download_url = path_shp_file_local, # local file does not require curl -- hacky + watch out, Url_to_spatialite will automatically delete the file 
                        overwrite = True #for some weird reason, appending duplicates the dataset -- even if we start of with a clean db
        ) as spatialite_etl:

        # Upload the point DB 
        spatialite_etl.upload_url_to_database()

    
    shp_test = spatialite_db_to_gdf( QcCityTestData.SPATIAL_LITE_DB_PATH,
     QcCityTestData.SPATIAL_LITE_TBL_QC
     )

    assert shp_qc.shape[0] == shp_test.shape[0]
 

def test_drop_table():

     # Make sure table exists initially
    if not os.path.exists(QcCityTestData.SPATIAL_LITE_DB_PATH):
        upload_qc_neigh_test_db()

    # Drop table 
    drop_table(QcCityTestData.SPATIAL_LITE_DB_PATH, QcCityTestData.SPATIAL_LITE_TBL_QC)

    # Table not present
    assert QcCityTestData.SPATIAL_LITE_TBL_QC not in list_tables(QcCityTestData.SPATIAL_LITE_DB_PATH)

    # Drop ALL
    drop_geo_table_all(QcCityTestData.SPATIAL_LITE_DB_PATH, QcCityTestData.SPATIAL_LITE_TBL_QC, QcCityTestData.SPATIAL_LITE_TBL_GEOMETRY_COL_NAME)
    list_aux_tables_to_drop_suff = ['','_rowid', '_node', '_parent']
    list_aux_tables_to_drop =  [ f"{QcCityTestData.SPATIAL_LITE_TBL_QC}_{QcCityTestData.SPATIAL_LITE_TBL_GEOMETRY_COL_NAME}{suffix};" \
                                 for suffix in list_aux_tables_to_drop_suff]
    assert not np.any(np.isin(np.array(list_aux_tables_to_drop), list_tables(QcCityTestData.SPATIAL_LITE_DB_PATH)   ))
   

if __name__ == '__main__':

    test_geometry_type()
    #test_spatial_index()