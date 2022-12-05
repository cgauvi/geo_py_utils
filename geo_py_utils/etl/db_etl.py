""" Class to interact with a spatil DB. Strongly inspired by https://github.com/Anagraph/yogrt/blob/main/yogrt/source.py"""


 
from os.path import isfile, isdir, join, exists, abspath
from os import listdir, environ
import subprocess
from abc import ABC
from pathlib import Path
import tempfile
import shutil
import atexit
import logging
import psycopg2
import pandas as pd

logger = logging.getLogger(__file__)


class Url_to_db(ABC):
    """Abstract base class for download of url and upload to db (spatialite, postgis)  
    
    Can work with potentially zipped geo files (shp, geojson) and upload to a db (spatialite, postgis) 
    Essentially a wrapper over:
        - curl
        - unzip
        - org2ogr

    """

    def __init__(self, 
                db_name, 
                table_name, 
                download_url, 
                download_destination = None,
                force_download: bool = False,
                overwrite: bool = False,
                target_projection: str = None):

        self.delete_download_destination = False
        if download_destination is None:
            download_destination = tempfile.mkdtemp()
            self.delete_download_destination = True

        self.db_name = db_name
        self.table_name = table_name
        self.download_url = download_url
        self.download_destination = download_destination
        self.force_download = force_download
        self.overwrite = overwrite
        self.target_projection = target_projection
        

        self.curl_download = None
        self.unzip_tmp_path = None

        atexit.register(self._cleanup)


    def _cleanup(self):

        try:
            shutil.rmtree(self.unzip_tmp_path)
            if self.delete_download_destination:
                shutil.rmtree(self.download_destination)
        except Exception as e:
            logger.warning(f"Warning, failed to delete temp directories up at cleanup")


    def _curl(self):

        self.curl_download = join(self.download_destination, self.table_name)

        if not exists(self.curl_download) or self.force_download:
            cmd =f"""curl {self.download_url} --output {self.curl_download}"""
            logger.info(cmd)
            p = subprocess.call(cmd, shell=True)
            subprocess.check_call(cmd, shell=True)
 

    def _try_unzip(self):

        try:
            self.unzip_tmp_path = tempfile.mkdtemp()
            self.path_src_to_upload = join(self.download_destination, self.unzip_tmp_path)

            if not exists(self.path_src_to_upload) or len(listdir(self.path_src_to_upload)) == 0:
                cmd = f"unzip -o {self.curl_download} -d {self.path_src_to_upload}"
                logger.info(cmd)
                p = subprocess.call(cmd, shell=True)
                subprocess.check_call(cmd, shell=True)
       

        except Exception as e:
            logger.info("{self.curl_download} does not seem to be a zipped file: {e} .. ")
            self.unzip_tmp_path = tempfile.mkstemp()
            self.path_src_to_upload = self.curl_download


    def upload_url_to_database(self):
        raise NotImplementedError("")


class Url_to_spatialite(Url_to_db):
    def __init__(self, 
                db_name,
                table_name, 
                download_url, 
                download_destination = None,
                force_download: bool = False,
                target_projection: str = None):

        super().__init__(db_name,
                        table_name, 
                        download_url, 
                        download_destination ,
                        force_download,
                        target_projection)


    def upload_url_to_database(self):
        self._curl()
        self._try_unzip()
        self._ogr2gr()


    def _ogr2gr(self):

        source = abspath(self.path_src_to_upload)
        dest = abspath(self.db_name)

        cmd = f"ogr2ogr " \
            " -progress " \
            " -f 'SQLite' " \
            " -dsco SPATIALITE=YES " \
            f" {dest} {source} "\
            f" -nlt PROMOTE_TO_MULTI "\
            f" -nln {self.table_name}"

        if self.overwrite:
            cmd = cmd + " -overwrite"
        else:
            cmd = cmd + " -append"

        if self.target_projection is not None:
            cmd = cmd + "-t_srs 'EPSG:{self.target_projection}'" 


        logger.info(cmd)
        p = subprocess.call(cmd, shell=True)
        subprocess.check_call(cmd, shell=True)
 




class Url_to_postgis(Url_to_db):

    def __init__(self, 
                db_name,
                table_name, 
                host,
                port,
                user,
                password,
                schema,
                download_url, 
                download_destination = None,
                force_download: bool = False,
                target_projection: str = None):
        
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.schema = schema

        super().__init__(db_name,
                        table_name, 
                        download_url, 
                        download_destination ,
                        force_download,
                        target_projection)

    
    def _ogr2gr(self):

        source = abspath(self.path_src_to_upload)
        dest = abspath(self.db_name)
        
        # Make sure db exists
        self._create_db()

        cmd = f"ogr2ogr  " \
            " -progress "\
            f" -f 'PostgreSQL' PG:'host={self.host} port={self.port} dbname={self.db_name} user={self.user} password={self.password}'" \
            f" -lco SCHEMA={self.schema} {source}" \
            f" -nlt PROMOTE_TO_MULTI  -nln {self.table_name} "

        if self.overwrite:
            cmd = cmd + "- overwrite"
        else:
            cmd = cmd + " -append"

        if self.target_projection is not None:
            cmd = cmd + "-t_srs 'EPSG:{self.target_projection}'" 


        logger.info(cmd)
        p = subprocess.call(cmd, shell=True)
        subprocess.check_call(cmd, shell=True)
 

    def _create_db(self):

        # Inspect all DBs
        with psycopg2.connect(
            database = "postgres", 
            user = self.user, 
            password = self.password , 
            host = self.host, 
            port = self.port
        ) as conn: 
            df_existing_dbs = pd.read_sql("SELECT datname FROM pg_database;", conn)
            
        # Db does not exist
        if self.db_name not in df_existing_dbs.datname.values:
            conn =  psycopg2.connect(
                database="postgres", 
                user = self.user, 
                password = self.password , 
                host = self.host, 
                port = self.port)

            conn.autocommit = True

            #Creating a cursor object using the cursor() method
            cursor = conn.cursor()

            #Preparing query to create a database - make sure it is a POSTGIS DB
            cursor.execute(f"CREATE database {self.db_name};")
            cursor.execute("CREATE EXTENSION postgis;")

            conn.close()

            logger.info(f"Successfully created postgis db {self.db_name}")


    def upload_url_to_database(self):
        self._curl()
        self._try_unzip()
        self._ogr2gr()


if __name__ == '__main__':

    from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
    from geo_py_utils.misc.constants import DATA_DIR

    # -----

    spatialite_db_path = join(DATA_DIR, "test.db")
    table_name = "qc_city_test_tbl"
    download_url = QC_CITY_NEIGH_URL

    uploader = Url_to_spatialite(
        db_name = spatialite_db_path, 
        table_name = table_name,
        download_url = download_url,
        download_destination = DATA_DIR)

    uploader.upload_url_to_database()


    # -----

    user = environ['PG_LOCAL_USER']
    password = environ['PG_LOCAL_PASSWORD']
    postgis_db_path = 'qc_city_db'

    uploader = Url_to_postgis(
            db_name = postgis_db_path, 
            table_name = table_name,
            download_url = download_url,
            download_destination = DATA_DIR, 
            host = "localhost",
            port = 5432,
            user = user, 
            password = password,
            schema = "public",
            )

    uploader.upload_url_to_database()