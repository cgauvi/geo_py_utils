 

from os.path import join
import geopandas as gpd
import os
import tempfile
import numpy as np
import pytest

from geo_py_utils.etl.db_etl import Url_to_postgis
from geo_py_utils.etl.spatialite.db_utils import (
    list_tables, 
    sql_to_df, 
    get_table_rows,
    get_table_crs,
    drop_table,
    drop_geo_table_all,
    is_spatial_index_enabled,
    is_spatial_index_valid,
    is_spatial_index_enabled_valid,
    get_table_geometry_type
)
from geo_py_utils.misc.constants import DATA_DIR
from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
from geo_py_utils.census_open_data.census import FSA_2016_URL
 


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

 