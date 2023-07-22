
from os.path import join
import sqlite3

from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.census_open_data.open_data import DEFAULT_QC_CITY_NEIGH_URL
from geo_py_utils.etl.db_etl import Url_to_spatialite
from geo_py_utils.etl.spatialite.gdf_load import spatialite_db_to_gdf


class QcCityTestData:
    SPATIAL_LITE_DB_PATH = join(DATA_DIR, "test.db")
    SPATIAL_LITE_TBL_QC = "qc_city_test_tbl"
    SQL_LITE_TBL_CSV_QC = 'csv_qc_city_test_tbl'
    QC_CITY_NEIGH_URL = DEFAULT_QC_CITY_NEIGH_URL
    SPATIAL_LITE_TBL_GEOMETRY_COL_NAME = 'GEOMETRY'



def upload_qc_neigh_test_db(db_name = QcCityTestData.SPATIAL_LITE_DB_PATH,
                        tbl_name = QcCityTestData.SPATIAL_LITE_TBL_QC,
                        download_url = QcCityTestData.QC_CITY_NEIGH_URL,
                        overwrite=True,
                        promote_to_multi=True):
    """Test helper to create a dummy test spatialite db

    Args:
        db_name (_type_, optional): _description_. Defaults to QcCityTestData.SPATIAL_LITE_DB_PATH.
        tbl_name (_type_, optional): _description_. Defaults to QcCityTestData.SPATIAL_LITE_TBL_QC.
        download_url (_type_, optional): _description_. Defaults to QcCityTestData.QC_CITY_NEIGH_URL.
        overwrite (bool, optional): _description_. Defaults to True.
        promote_to_multi (bool, optional): _description_. Defaults to True.
    """
    
    with Url_to_spatialite(
        db_name = db_name, 
        table_name = tbl_name,
        download_url = download_url,
        download_destination = DATA_DIR,
        overwrite=overwrite, 
        promote_to_multi=promote_to_multi) as uploader:

        uploader.upload_url_to_database()


def write_regular_csv_db(db_name = QcCityTestData.SPATIAL_LITE_DB_PATH,
                        tbl_name_init = QcCityTestData.SPATIAL_LITE_TBL_QC,
                        tbl_name_csv =  QcCityTestData.SQL_LITE_TBL_CSV_QC):

    """Test helper to write a regular csv to spatialite 

    Args:
        db_name (_type_, optional): _description_. Defaults to QcCityTestData.SPATIAL_LITE_DB_PATH.
        tbl_name_init (_type_, optional): _description_. Defaults to QcCityTestData.SPATIAL_LITE_TBL_QC.
        tbl_name_csv (str, optional): _description_. Defaults to 'tbl_csv_test'.
    """


    shp_init = spatialite_db_to_gdf(db_name=db_name, 
                                    tbl_name=tbl_name_init, 
                                    current_geo_col_name=QcCityTestData.SPATIAL_LITE_TBL_GEOMETRY_COL_NAME)

    df = shp_init.\
        drop(columns=QcCityTestData.SPATIAL_LITE_TBL_GEOMETRY_COL_NAME.lower()).\
        sample(n=min(10, shp_init.shape[0]))

    with sqlite3.connect(db_name) as conn:
        df.to_sql(tbl_name_csv, conn)

    

        

    
