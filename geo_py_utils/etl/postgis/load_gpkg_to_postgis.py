
from os.path import exists
import logging
import subprocess
from pathlib import Path
from os.path import abspath, dirname

from geo_py_utils.etl.postgis.db_utils import pg_create_ogr2ogr_str

# Logger
logger = logging.getLogger(__file__)


class LoadGPKGPostgis:

    def __init__(self,
                gpkg_name,
                database,
                host,
                port,
                user,
                password,
                schema='public',
                overwrite=True,
                promote_to_multi=False):

 
        self.gpkg_name = gpkg_name
        self.postgis_db_name = database
 
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        
        self.schema = schema
 
        self.overwrite = overwrite
        self.promote_to_multi = promote_to_multi
 
        # Create the ogr2ogr connection string for the db
        self.ogr2ogr_dest_connection_str = pg_create_ogr2ogr_str(
            database = self.postgis_db_name,
            user = self.user , 
            password = self.password,
            host = self.host,
            port = self.port
        )

    def _ogr2ogr(self) -> str :
        """Build the ogr2ogr string 

        Returns:
            str:  ogr2ogr string to call with e.g. `subprocess.check_call`
        """

        # Basic commands
        cmd = "ogr2ogr  " \
            ' -progress ' \
            ' --config PG_USE_COPY YES ' 


        # ogr2ogr format: Dest Source
        # See https://gdal.org/programs/ogr2ogr.html
        cmd += fr"  {self.ogr2ogr_dest_connection_str} '{self.gpkg_name}' " 

        # Additional commands 
        cmd += " -lco ENCODING=UTF-8 " 

        if self.promote_to_multi:
            cmd += " -nlt PROMOTE_TO_MULTI "
    
        return cmd


    def upload_url_to_database(self):
        """Extract parcel data and load into a spatialite DB
        """

        # QA
        assert exists(self.gpkg_name)
 
   
        # Call ogr2ogr
        ogr_cmd = self._ogr2ogr()

        # Actual system call
        logger.info(ogr_cmd)
        subprocess.check_call(ogr_cmd, shell=True)  
 

        logger.info(f"Successfully uploaded entire gpkg {self.gpkg_name} to {self.host}:{self.port}/{self.postgis_db_name}")


if __name__ == '__main__' :

    from geo_py_utils.etl.postgis.postgis_connection import PostGISDBConnection

    # Connection object to postgis dev table 
    path_gic_root = Path(abspath(dirname(__file__))).parent.parent.parent.parent.parent
    dev_db_cred = path_gic_root / "config" / "dev" / ".env"
    assert exists(dev_db_cred)
    db_connector = PostGISDBConnection(dev_db_cred)

    # Point to small gpkg
    gpkg_vexcel_only = path_gic_root / "data" / "gic_backup_vexcel_only.gpkg"
    assert exists(gpkg_vexcel_only)

    # Upload 
    loader = LoadGPKGPostgis( 
        gpkg_name=gpkg_vexcel_only,
        schema='public',
        overwrite=True,
        promote_to_multi=False,
        **db_connector.get_credentials()
        )

    loader.upload_url_to_database()