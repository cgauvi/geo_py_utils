from os.path import join
import geopandas as gpd
import os
import tempfile
import numpy as np
import pytest
import subprocess
import logging
from sqlalchemy import create_engine


logger = logging.getLogger(__file__)

from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
from geo_py_utils.census_open_data.census import FSA_2016_URL
from geo_py_utils.etl.db_etl import Url_to_postgis
from geo_py_utils.etl.port import is_port_open
from geo_py_utils.etl.postgis.load_sfkl_to_postgis import LoadSfklPostgis
from geo_py_utils.etl.postgis.load_spatialite_to_postgis import LoadSqlitePostgis
from geo_py_utils.etl.postgis.db_utils import pg_db_exists, pg_create_db



class LocalPostGIS:
    USER = os.getenv('PG_LOCAL_USER') # defaults to None if not set
    PASSWORD = os.getenv('PG_LOCAL_PASSWORD')
    POSTGIS_DB = 'qc_city_db'
    HOST = "localhost"
    PORT = 5432
    SCHEMA = "public"

class LocalDockerPostGIS:
    USER = os.getenv('PG_GIC_USER') # defaults to None if not set
    PASSWORD = os.getenv('PG_GIC_PASSWORD')
    POSTGIS_DB = 'gis'
    HOST = "localhost"
    PORT = 80
    SCHEMA = "public"

class RemotePostGIS:
    USER = os.getenv('PG_GIC_USER') # defaults to None if not set
    PASSWORD = os.getenv('PG_GIC_PASSWORD')
    POSTGIS_DB = 'gis'
    HOST = os.getenv('PG_GIC_HOST')
    PORT = 5052
    SCHEMA = "public"


 



def test_spatialite_local_to_postgis_remote():

    engine = create_engine('postgresql://{RemotePostGIS.USER}:{RemotePostGIS.PASSWORD}@{RemotePostGIS.HOST}:{RemotePostGIS.PORT}/{RemotePostGIS.POSTGIS_DB}')
    
    if not pg_db_exists(engine, db_name):
        pg_create_db(db_name=RemotePostGIS.POSTGIS_DB,
                    user=RemotePostGIS.USER, 
                    password=RemotePostGIS.PASSWORD,
                    host=RemotePostGIS.HOST, 
                    port=RemotePostGIS.PORT)

    # Administrative regions
    sqlite_postgis_loader  = LoadSqlitePostgis (
        sqlite_tbl_name="geo_adm_regions",
        postgis_tbl_name = 'gic_geo_adm_regions',
        host = RemotePostGIS.HOST,
        port = RemotePostGIS.PORT,
        user = RemotePostGIS.USER,
        password = RemotePostGIS.PASSWORD,
        schema = RemotePostGIS.SCHEMA,
        promote_to_multi=False,
        overwrite=True
        )

    sqlite_postgis_loader.upload_url_to_database()


    # Try a regular table 
    sqlite_postgis_loader = LoadSqlitePostgis(
        sqlite_tbl_name="tbl_muni",
        postgis_tbl_name = 'gic_tbl_muni_pc_map',
        host = RemotePostGIS.HOST,
        port = RemotePostGIS.PORT,
        user = RemotePostGIS.USER,
        password = RemotePostGIS.PASSWORD,
        schema = RemotePostGIS.SCHEMA,
        promote_to_multi=False,
        overwrite=True
        )

    sqlite_postgis_loader.upload_url_to_database()


def test_sfkl_to_postgis_local_docker():
    
    if not is_port_open(LocalDockerPostGIS.HOST, LocalDockerPostGIS.PORT):
        logger.warning(f'Host: {LocalDockerPostGIS.HOST}:{LocalDockerPostGIS.PORT} seems closed -> skipping test')
        return 

    # Dissemination areas
    sfkl_postgis_loader_agg_tbl = LoadSfklPostgis(
        sfkl_tbl_name="GEOSPATIAL_TEST_BUGS",
        postgis_tbl_name = 'GEO_BUGS',
        host = LocalDockerPostGIS.HOST,
        port = LocalDockerPostGIS.PORT,
        user = LocalDockerPostGIS.USER,
        password = LocalDockerPostGIS.PASSWORD,
        schema = LocalDockerPostGIS.SCHEMA,
        promote_to_multi=False,
        overwrite=True
        )

    sfkl_postgis_loader_agg_tbl.upload_url_to_database()
 



def test_local_shp_to_postgis_local_db_qc():
    """Run test against a local DB - presumably on local linux machine
    """

    psql_exists_results = subprocess.run(["which","psql"], stdout=subprocess.PIPE)
    if len(psql_exists_results.stdout) > 0:


        with Url_to_postgis(
            db_name = postgis_db_path, 
            table_name = "qc_city_test_tbl",
            download_url = QC_CITY_NEIGH_URL,
            download_destination = DATA_DIR,
            host = LocalPostGIS.HOST,
            port = LocalPostGIS.PORT,
            user = LocalPostGIS.USER,
            password = LocalPostGIS.PASSWORD,
            schema = LocalPostGIS.SCHEMA,
            ) as uploader:

            if uploader._port_is_open():
                uploader.upload_url_to_database()
            else:
                logger.warning("Skipping test_postgis: port seems closed")
    else:
        logger.warning("Skipping test_postgis: did not find psql on system")

 