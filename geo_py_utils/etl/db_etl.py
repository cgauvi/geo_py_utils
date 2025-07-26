""" Class to interact with a spatil DB. Strongly inspired by https://github.com/Anagraph/yogrt/blob/main/yogrt/source.py"""


from os.path import exists
from os import listdir, makedirs, remove
 
from abc import ABC
from pathlib import Path, PurePosixPath, WindowsPath
import tempfile
import shutil
import logging
from sqlalchemy import create_engine
import sys

from geo_py_utils.etl.port import is_port_open
from geo_py_utils.etl.postgis.db_utils import pg_create_db, pg_db_exists
from geo_py_utils.misc.cli_runner import CLIRunner
from geo_py_utils.misc.cli_windows import ogr2ogr_windows_wrapper

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
                layer_name = None,
                download_destination = None,
                force_download: bool = False,
                overwrite: bool = False,
                target_projection: str = None,
                remove_tmp_download_files = True,
                no_overwrite_append = False,
                promote_to_multi=True,
                new_geom_name=None):

        # Download and unzipped directories 
        # Use tmp dir + delete at the edn
        if download_destination is None:
            download_destination = tempfile.mkdtemp()
            path_src_to_upload = tempfile.mkdtemp() # point to dir where we will extract the data
            self.delete_download_destination = True
        else:
            path_src_to_upload = Path(download_destination) / f"{table_name}_post_curl"
            makedirs(path_src_to_upload, exist_ok = True)
            self.delete_download_destination = False

        self.curl_download =  Path(download_destination) / f"{table_name}_curl"
        self.path_src_to_upload = path_src_to_upload

        self.new_geom_name = new_geom_name
        self.layer_name = layer_name
        self.db_name = Path(db_name)
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
                cmd =f"curl '{PurePosixPath(self.download_url)}' --output '{PurePosixPath(self.curl_download)}'" # always use curl + bash (no equivalent in powershell)
                logger.info(cmd)
                CLIRunner(use_bash=True, capture_std_err=True).run_cmd(cmd)
            except Exception as e:
                
                if exists(self.download_url):
                    logger.info(f"Failed to run curl: {self.download_url} seems to be a local file/folder rather than a valid URL ... {e} ")
                    self.curl_download = self.download_url # point to the local file
 

    def _try_unzip(self):

        try:
            if not exists(self.path_src_to_upload) or len(listdir(self.path_src_to_upload)) == 0:
                cmd = f"unzip -o '{PurePosixPath(self.curl_download)}' -d '{PurePosixPath(self.path_src_to_upload)}'" # unzip + bash
                logger.info(cmd)
                CLIRunner().run_cmd(cmd)

            # We might have unzipped a new folder xxx to self.path_src_to_upload/xxx
            # Need to point to self.path_src_to_upload/xxx
            # This only works for 2 use cases, but seems impossibly complex to code in general if there are more than 1 level of nested directories
            sub_dirs = listdir(self.path_src_to_upload)
            if len(sub_dirs) == 1:
                self.path_src_to_upload = Path(self.path_src_to_upload) / sub_dirs[0]
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

        source =  WindowsPath(self.path_src_to_upload) if 'win' in sys.platform else PurePosixPath(self.path_src_to_upload)
        dest = WindowsPath(self.db_name) if 'win' in sys.platform else PurePosixPath(self.db_name)

        cmd = "ogr2ogr " \
            " -progress " \
            " -f 'SQLite' " \
            " -dsco SPATIALITE=YES "

        if self.target_projection is not None:
            cmd += f"-t_srs EPSG:{self.target_projection}" 

        cmd  +=  f' "{dest}" "{source}" '\
            f" -nln {self.table_name}" \
            " -lco ENCODING=UTF-8 " 
        
        if self.new_geom_name is not None:
            cmd += f"-lco GEOMETRY_NAME='{self.new_geom_name}'"

        if self.promote_to_multi:
             cmd += " -nlt PROMOTE_TO_MULTI "

        if not self.no_overwrite_append and not self.first_time_creating_db:
            if self.overwrite:
                cmd += " -overwrite"
            else:
                cmd += " -update"

        # This can be useful with e.g. fgdb where there are multiple layers that could be written as tables
        if self.layer_name is not None:
            cmd += f" {self.layer_name} "

        # Make sure we are starting from scratch
        # Avoid weird bugs where query to non-existent db creates the file and trying to write after creates error
        if self.first_time_creating_db:
            if exists(dest):
                remove(dest)

        # Actual ogr2ogr call
        ogr2ogr_windows_wrapper(cmd)

        
 




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
        source = WindowsPath(self.path_src_to_upload) if 'win' in sys.platform else PurePosixPath(self.path_src_to_upload)
        if self.src_spatialite_tbl_name is not None:
            cmd += fr" '{source}' '{self.src_spatialite_tbl_name}' " 
        else:
            cmd += fr" '{source}' " 

        cmd += f' -lco SCHEMA={self.schema} ' \
            f" -nln {self.table_name} " \
            " -lco ENCODING=UTF-8 " 

        if self.new_geom_name is not None:
            cmd += f"-lco GEOMETRY_NAME='{self.new_geom_name}'"

        if self.promote_to_multi:
             cmd +=  " -nlt PROMOTE_TO_MULTI "

        if not self.no_overwrite_append and not self.first_time_creating_db:
            if self.overwrite:
                cmd += " -overwrite"
            else:
                cmd += " -append"

        # Actual ogr2ogr call
        ogr2ogr_windows_wrapper(cmd)
 

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

 