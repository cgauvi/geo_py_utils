""" Class to interact with a spatil DB. Strongly inspired by https://github.com/Anagraph/yogrt/blob/main/yogrt/source.py"""


 
from os.path import join, exists, abspath
from os import listdir, environ, makedirs
import subprocess
from abc import ABC
from pathlib import Path
import tempfile
import shutil
import logging
from sqlalchemy import create_engine


from geo_py_utils.etl.port import is_port_open
from geo_py_utils.etl.postgis.db_utils import pg_create_db, pg_db_exists

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
                target_projection: str = None,
                remove_tmp_download_files = True,
                no_overwrite_append = False,
                promote_to_multi=True):

        # Download and unzipped directories 
        # Use tmp dir + delete at the edn
        if download_destination is None:
            download_destination = tempfile.mkdtemp()
            path_src_to_upload = tempfile.mkdtemp() # point to dir where we will extract the data
            self.delete_download_destination = True
        else:
            path_src_to_upload = join(download_destination, f"{table_name}_post_curl")
            makedirs(path_src_to_upload, exist_ok = True)
            self.delete_download_destination = False

        self.curl_download = join(download_destination, f"{table_name}_curl")
        self.path_src_to_upload = path_src_to_upload


        self.db_name = db_name
        self.table_name = table_name
        self.download_url = download_url
        self.download_destination = download_destination
        self.force_download = force_download
        self.overwrite = overwrite
        self.target_projection = target_projection
        self.remove_tmp_download_files = remove_tmp_download_files # if download_destination is not None, this has precedence and we dont delete the folder 
        self.no_overwrite_append = no_overwrite_append
        self.promote_to_multi = promote_to_multi



    @staticmethod
    def _safe_delete(f):
        """Delete a single file or directory if it exists.
        """

        if exists(f) :
            try:
                Path(f).unlink(missing_ok = True) #os.remove gives some weird errors
            except Exception:
                try:
                    shutil.rmtree(f)
                except Exception as e:
                    raise e(f"Fatal error trying to delete {f}")


    def _safe_delete_all_files(self):
        """ Delete unnecessary tmp files
        """ 

        # Always delete the files downloaded by curl 
        files_always_delete = [self.curl_download]
        for f in files_always_delete:
            try:
                Url_to_db._safe_delete(f)
            except Exception as e:
                logger.warning(f"Warning, failed to delete temp directories {f} up at cleanup - {e}")
                pass 

        # If we inputed a download_destination: keep everything
        try:
            if self.delete_download_destination and exists(self.download_destination):
                Url_to_db._safe_delete(self.download_destination)
                Url_to_db._safe_delete(self.path_src_to_upload)
        except Exception as e:
            logger.warning(f"Warning, failed to delete temp download dir up at cleanup - {e}")


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        if self.remove_tmp_download_files:
            self._safe_delete_all_files()
        else:
            logger.warning("Warning! not deleting temprary download files! make sure to not clog your system")

    def _curl(self):

        if not exists(self.curl_download) or self.force_download:
            try:
                cmd =f"""curl {self.download_url} --output {self.curl_download}"""
                logger.info(cmd)
                subprocess.check_call(cmd, shell=True)
            except Exception as e:
                
                if exists(self.download_url):
                    logger.info(f"Failed to run curl: {self.download_url} seems to be a local file/folder rather than a valid URL ... {e} ")
                    self.curl_download = self.download_url # point to the local file
 

    def _try_unzip(self):

        try:
            if not exists(self.path_src_to_upload) or len(listdir(self.path_src_to_upload)) == 0:
                cmd = f"unzip -o {self.curl_download} -d {self.path_src_to_upload}"
                logger.info(cmd)
                subprocess.check_call(cmd, shell=True)

            # We might have unzipped a new folder xxx to self.path_src_to_upload/xxx
            # Need to point to self.path_src_to_upload/xxx
            # This only works for 2 use cases, but seems impossibly complex to code in general if there are more than 1 level of nested directories
            sub_dirs = listdir(self.path_src_to_upload)
            if len(sub_dirs) == 1:
                self.path_src_to_upload = join(self.path_src_to_upload, sub_dirs[0])
                logger.debug(f"In _try_unzip: {self.path_src_to_upload} contains a single sub dir/files -> point to {sub_dirs[0]}")
            elif len(sub_dirs) > 1:
                logger.debug(f"In _try_unzip: {self.path_src_to_upload} contains many directories -> point to {self.path_src_to_upload}")

        except Exception as e:
            logger.info(f"{self.curl_download} does not seem to be a zipped file: {e} .. ")
            self.path_src_to_upload = self.curl_download # point to the file we just downloaded with curl


    def upload_url_to_database(self):
        raise NotImplementedError("")


class Url_to_spatialite(Url_to_db):
    def __init__(self, **kwargs):

        super().__init__(**kwargs)

        self.first_time_creating_db = True if not exists(self.db_name) else False # check existence BEFORE running any queries since opening connection to db creates it by default which is undesired


    def upload_url_to_database(self):
        self._curl()
        self._try_unzip()
        self._ogr2gr()


    def _ogr2gr(self):

        source = abspath(self.path_src_to_upload)
        dest = abspath(self.db_name)

        cmd = "ogr2ogr " \
            " -progress " \
            " -f 'SQLite' " \
            " -dsco SPATIALITE=YES "

        if self.target_projection is not None:
            cmd += f"-t_srs EPSG:{self.target_projection}" 

        cmd  +=  f" {dest} {source} "\
            f" -nln {self.table_name}" \
            " -lco ENCODING=UTF-8 " 

        if self.promote_to_multi:
             cmd +=  " -nlt PROMOTE_TO_MULTI "

        if not self.no_overwrite_append and not self.first_time_creating_db:
            if self.overwrite:
                cmd += " -overwrite"
            else:
                cmd += " -append"

        # Make sure we are starting from scratch
        # Avoid weird bugs where query to non-existent db creates the file and trying to write after creates error
        if self.first_time_creating_db:
            if exists(dest):
                subprocess.check_call(f"rm {dest}", shell=True)

        # Actual ogr2ogr call
        logger.info(cmd)
        subprocess.check_call(cmd, shell=True)
 




class Url_to_postgis(Url_to_db):

    def __init__(self, 
                host,
                port,
                user,
                password,
                schema,
                src_spatialite_tbl_name=None,
                **kwargs):

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.schema = schema
        self.src_spatialite_tbl_name = src_spatialite_tbl_name

        super().__init__( **kwargs)

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{self.db_name}')
        self.first_time_creating_db = True if not pg_db_exists(engine, self.db_name) else False 

    
    def _ogr2gr(self):

        
        
        # Make sure db exists
        self._create_db()

        cmd = "ogr2ogr  " \
            ' -progress ' \
            ' --config PG_USE_COPY YES ' 

        if self.target_projection is not None:
            cmd += f"-t_srs 'EPSG:{self.target_projection}'" 

        # Target DB
        cmd += fr"  -f 'PostgreSQL' PG:'host={self.host} port={self.port} dbname={self.db_name} user={self.user} password={self.password}'" 
            
        # Source DB
        # Special treatment fro spatialite to postgis
        # Hackish: only works by manually setting self.path_src_to_upload
        source = abspath(self.path_src_to_upload)
        if self.src_spatialite_tbl_name is not None:
            cmd += fr" {source} {self.src_spatialite_tbl_name} " 
        else:
            cmd += fr" {source} " 

        cmd += f' -lco SCHEMA={self.schema} ' \
            f" -nln {self.table_name} " \
            " -lco ENCODING=UTF-8 " 

        if self.promote_to_multi:
             cmd +=  " -nlt PROMOTE_TO_MULTI "

        if not self.no_overwrite_append and not self.first_time_creating_db:
            if self.overwrite:
                cmd += " -overwrite"
            else:
                cmd += " -append"

        logger.info(cmd)
        subprocess.check_call(cmd, shell=True)
 

    def _create_db(self):

        # Inspect all DBs
        pg_create_db(
            database= self.db_name, 
            user = self.user, 
            password = self.password , 
            host = self.host, 
            port = self.port
        )  


    def _port_is_open(self):

        return is_port_open(self.host, self.port)


    def upload_url_to_database(self):
        self._curl()
        self._try_unzip()
        self._ogr2gr()


if __name__ == '__main__':

    from geo_py_utils.census_open_data.open_data import QC_CITY_NEIGH_URL
    from geo_py_utils.misc.constants import DATA_DIR
    from geo_py_utils.census_open_data.census import FSA_2016_URL
    from geo_py_utils.etl.gdf_load import spatialite_db_to_gdf
    from geo_py_utils.etl.db_utils import sql_to_df

    import os
    import geopandas as gpd

    # --

    spatialite_db_path = join(DATA_DIR, "test.db")
    table_name = "qc_city_test_tbl"
    download_url = QC_CITY_NEIGH_URL

    # --


    if os.path.exists(spatialite_db_path):
        os.remove(spatialite_db_path)

    # Read the shp file from the url + write to disk before uploading to spatialite
    shp_qc = gpd.read_file(QC_CITY_NEIGH_URL)
    tmp_dir = tempfile.mkdtemp()
    path_shp_file_local = join(tmp_dir, 'tmp.shp')
    shp_qc.to_file(path_shp_file_local)

    with Url_to_spatialite(
                        db_name = spatialite_db_path, 
                        table_name = table_name,
                        download_url = path_shp_file_local # local file does not require curl -- hacky 
        ) as spatialite_etl:

        # Upload the point DB 
        spatialite_etl.upload_url_to_database()

    
    shp_test = spatialite_db_to_gdf(spatialite_db_path, table_name)
    os.remove(path_shp_file_local)



    # --

    shp_test = spatialite_db_to_gdf(join(DATA_DIR, "test_fsa.db"),  
     'geo_fsa_tbl',
     'limit 10'
     )

    with Url_to_spatialite(
        db_name = join(DATA_DIR, "test_fsa.db"), 
        table_name = 'geo_fsa_tbl',
        download_url = FSA_2016_URL,
        download_destination = DATA_DIR,
        target_projection=32198) as uploader:

        uploader.upload_url_to_database()

    shp_test = spatialite_db_to_gdf(join(DATA_DIR, "test_fsa.db"),  
     'geo_fsa_tbl',
     'limit 10'
     )

    shp_test
    
    sql_to_df(join(DATA_DIR, "test_fsa.db"), 'select * from spatial_ref_sys where srid IN (32198, 325834)')
    sql_to_df(join(DATA_DIR, "test_fsa.db"), 'select * from geometry_columns')

    # ----


    with Url_to_spatialite(
        db_name = join(DATA_DIR, "test_role_eval.db"), 
        table_name = 'geo_role_tbl',
        download_url = "https://donneesouvertes.affmunqc.net/role/ROLE2020_SHP.zip",
        download_destination = DATA_DIR) as uploader:

        uploader.upload_url_to_database()

    # -----



    with Url_to_spatialite(
        db_name = spatialite_db_path, 
        table_name = table_name,
        download_url = download_url,
        download_destination = DATA_DIR) as uploader:

        uploader.upload_url_to_database()


    # -----

    user = environ['PG_LOCAL_USER']
    password = environ['PG_LOCAL_PASSWORD']
    postgis_db_path = 'qc_city_db'

    with Url_to_postgis(
            db_name = postgis_db_path, 
            table_name = table_name,
            download_url = download_url,
            download_destination = DATA_DIR, 
            host = "localhost",
            port = 5432,
            user = user, 
            password = password,
            schema = "public",
            ) as uploader:


        uploader.upload_url_to_database()