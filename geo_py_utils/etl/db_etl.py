""" Class to interact with a spatil DB. Strongly inspired by https://github.com/Anagraph/yogrt/blob/main/yogrt/source.py"""


import os
import subprocess
from abc import ABC
from pathlib import Path

class db_io(ABC):
    """Abstract base class for download of zipped geo files (shp, geojson) and upload to a db (spatialite, postgis) thruh org2ogr
    """

    def __init__(self, 
                db_name, 
                table_name, 
                download_url, 
                destination_folder = None,
                unzip_filename: str = None, 
                force_download: bool = False,
                overwrite: bool = True,
                target_projection: str = None):

        if destination_folder is None:
            destination_folder = os.path.curdir()

        if unzip_filename is None:
            unzip_filename = Path(download_url).stem

        if table_name is None:
            table_name = unzip_filename

        self.table_name = table_name
        self.download_url = download_url
        self.destination_folder = destination_folder
        self.unzip_filename = unzip_filename
        self.downloaded_path = None
        self.force_download = force_download
        self.overwrite = overwrite
        self.target_projection = target_projection
        self.db_name = db_name


    def _download_cmd(self):
        cmd =f"""curl {self.download_url} --output {self.destination_folder}"""
        print(cmd)
        p = subprocess.Popen(cmd, shell=True)
        p.wait()


    def download_unzip_cmd(self):

        final_unzip = os.path.join(self.destination_folder, self.unzip_filename)

        if self.force_download or not os.path.exists(final_unzip):

            self._download_cmd()

            cmd = f"unzip -o {self.downloaded_path} -d {self.destination_folder}"
            print(cmd)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()
        
        self.downloaded_path = final_unzip

    def import_to_database(self):
        raise NotImplementedError("")


class spatialite_io(db_io):
    def __init__(self, 
                db_name,
                table_name, 
                download_url, 
                destination_folder = None,
                unzip_filename: str = None, 
                force_download: bool = False,
                target_projection: str = None):

        super().__init__(db_name,
                        table_name, 
                        download_url, 
                        destination_folder ,
                        unzip_filename , 
                        force_download,
                        target_projection)

    def import_to_database(self):

        cmd = f"""ogr2ogr 
        -progress 
        -f SQLite 
        -dscp SPATIALITE=YES
        -nlt PROMOTE_TO_MULTI 
        -nln {self.table_name} {self.downloaded_path}"""

        if self.overwrite:
            cmd = cmd + "-overwrite"

        if self.target_projection is not None:
            cmd = cmd + "-t_srs 'EPSG:{self.target_projection}'" 


        print(cmd)
        p = subprocess.Popen(cmd, shell=True)
        p.wait()




class postgis_io(db_io):

    def __init__(self, 
                db_name,
                table_name, 
                host,
                port,
                user,
                password,
                schema,
                download_url, 
                destination_folder = None,
                unzip_filename: str = None, 
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
                        destination_folder ,
                        unzip_filename , 
                        force_download)

    
    def import_to_database(self):

        cmd = f"""ogr2ogr 
            -progress 
            -f "PostgreSQL" PG:"host='{self.host}' port='{self.port}' dbname='{self.db_name}' user='{self.user}' password='{self.password}'" -lco SCHEMA={self.schema}
            -dscp SPATIALITE=YES
            -nlt PROMOTE_TO_MULTI 
            -nln {self.table_name} {self.downloaded_path}"""

        if self.overwrite:
            cmd = cmd + "-overwrite"

        if self.target_projection is not None:
            cmd = cmd + "-t_srs 'EPSG:{self.target_projection}'" 


        print(cmd)
        p = subprocess.Popen(cmd, shell=True)
        p.wait()

 