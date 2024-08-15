# Utilities to write mock data to a local spatialite db 

import logging
import os
import sqlite3
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon

from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.utilities_testing.data import QcCityTestData, LocalGDFData
from geo_py_utils.etl.db_etl import Url_to_spatialite
from geo_py_utils.etl.spatialite.gdf_load import spatialite_db_to_gdf

logger = logging.getLogger(__file__)


def create_local_spatialite( 
        db_name = LocalGDFData.SPATIAL_LITE_DB_PATH,
        tbl_name = LocalGDFData.SPATIAL_LITE_TBL_QC,
        overwrite=True,
        promote_to_multi=True
        ):
    
    if overwrite and os.path.exists(db_name):
        os.chmod(db_name, 0o777)
        os.remove(db_name)

    if promote_to_multi:
        shp = LocalGDFData.DATA.copy()
        shp["geometry"] = [MultiPolygon([feature]) if isinstance(feature, Polygon) and k == 1  \
            else feature for k, feature in enumerate(shp["geometry"])]

    if not os.path.exists(db_name):
        shp.to_file(
            db_name, 
            driver="SQLite", 
            spatialite=True, 
            layer=tbl_name
            )


def upload_qc_neigh_test_db(
        db_name = QcCityTestData.SPATIAL_LITE_DB_PATH,
        tbl_name = QcCityTestData.SPATIAL_LITE_TBL_QC,
        download_url = QcCityTestData.QC_CITY_NEIGH_URL,
        overwrite=True,
        promote_to_multi=True
    ):
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


def upload_regular_csv_db(
        db_name = QcCityTestData.SPATIAL_LITE_DB_PATH,
        tbl_name_init = QcCityTestData.SPATIAL_LITE_TBL_QC,
        tbl_name_csv =  QcCityTestData.SQL_LITE_TBL_CSV_QC
    ):

    """Test helper to write a regular csv to spatialite 

    Args:
        db_name (_type_, optional): _description_. Defaults to QcCityTestData.SPATIAL_LITE_DB_PATH.
        tbl_name_init (_type_, optional): _description_. Defaults to QcCityTestData.SPATIAL_LITE_TBL_QC.
        tbl_name_csv (str, optional): _description_. Defaults to 'tbl_csv_test'.
    """

    shp_init = spatialite_db_to_gdf(
        db_name=db_name, 
        tbl_name=tbl_name_init, 
        current_geo_col_name=QcCityTestData.SPATIAL_LITE_TBL_GEOMETRY_COL_NAME
    )

    df = shp_init.\
        drop(columns=QcCityTestData.SPATIAL_LITE_TBL_GEOMETRY_COL_NAME.lower()).\
        sample(n=min(10, shp_init.shape[0]))
    
    conn= sqlite3.connect(db_name)
    df.to_sql(tbl_name_csv, conn)
    conn.close()

    
    logger.info(f'Writing table {tbl_name_init} to db {db_name}')

    

if __name__== '__main__':
    upload_regular_csv_db()