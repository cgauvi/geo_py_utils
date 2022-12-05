""" Class to interact with a spatil DB. Strongly inspired by https://github.com/Anagraph/yogrt/blob/main/yogrt/source.py"""


 
from os.path import isfile, isdir, join, exists
import subprocess
from abc import ABC
from pathlib import Path
import tempfile
import shutil
import atexit
import logging

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
            logger.warn(f"Warning, failed to delete temp directories up at cleanup")


    def _curl(self):

        self.curl_download = join(self.download_destination, self.table_name)

        if not exists(self.curl_download) or self.force_download:
            cmd =f"""curl {self.download_url} --output {self.curl_download}"""
            print(cmd)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()

    def _try_unzip(self):

        try:
            self.unzip_tmp_path = tempfile.mkdtemp()
            self.path_src_to_upload = join(self.download_destination, self.unzip_tmp_path)

            if not exists(self.path_src_to_upload):
                cmd = f"unzip -o {self.curl_download} -d {self.path_src_to_upload}"
                print(cmd)
                p = subprocess.Popen(cmd, shell=True)
                p.wait()

        except Exception as e:
            logger.info("{self.curl_download} does nt seem to be a zipped file: {e} .. ")
            self.unzip_tmp_path = tempfile.mkstemp()
            self.path_src_to_upload = join(self.download_destination, self.unzip_tmp_path)


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

        source = self.curl_download
        dest = self.db_name

        cmd = f"""ogr2ogr 
        -progress 
        -f 'SQLite' 
        -dsco SPATIALITE=YES
        -nlt PROMOTE_TO_MULTI 
        {dest} {source}
        -nln {self.table_name} """

        if self.overwrite:
            cmd = cmd + "-overwrite"
        else:
            cmd = cmd + "-append"

        if self.target_projection is not None:
            cmd = cmd + "-t_srs 'EPSG:{self.target_projection}'" 


        print(cmd)
        p = subprocess.Popen(cmd, shell=True)
        p.wait()




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

        source = self.curl_download
        dest = self.db_name

        cmd = f"""ogr2ogr 
        -progress 
         -f "PostgreSQL" PG:"host='{self.host}' port='{self.port}' dbname='{self.db_name}' user='{self.user}' password='{self.password}'" -lco SCHEMA={self.schema}
           
        -dsco SPATIALITE=YES
        -nlt PROMOTE_TO_MULTI 
        {dest} {source}
        -nln {self.table_name} """

        if self.overwrite:
            cmd = cmd + "-overwrite"
        else:
            cmd = cmd + "-append"

        if self.target_projection is not None:
            cmd = cmd + "-t_srs 'EPSG:{self.target_projection}'" 


        print(cmd)
        p = subprocess.Popen(cmd, shell=True)
        p.wait()


    def upload_url_to_database(self):
        self._curl()
        self._try_unzip()
        self._ogr2gr()


if __name__ == '__main__':

    from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
    from geo_py_utils.misc.constants import DATA_DIR

    spatialite_db_path = join(DATA_DIR, "test.db")
    table_name = "qc_city_test_tbl"
    download_url = QC_CITY_NEIGH_URL

    Url_to_spatialite(
        db_name = spatialite_db_path, 
        table_name = table_name,
        download_url = download_url,
        download_destination = DATA_DIR)
